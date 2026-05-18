-- batch_deduct_stock.lua
-- KEYS: KEYS[1..n] 库存 key, KEYS[n+1] Redis outbox key, KEYS[n+2] epoch key, KEYS[n+3] seq key
-- ARGV: ARGV[1..n] 扣减数量, ARGV[n+1] orderId, ARGV[n+2] 序列化消息 JSON

--local item_count = #KEYS - 3
--local outbox_key = KEYS[item_count + 1]
--local epoch_key = KEYS[item_count + 2]
--local seq_key = KEYS[item_count + 3]
--local order_id = ARGV[item_count + 1]
--local payload = ARGV[item_count + 2]
--
---- 0. 订单维度幂等：重复 orderId 直接放行，避免重复预扣 Redis 库存
--if redis.call('HEXISTS', outbox_key, order_id) == 1 then
--    return 0
--end
--
--local epoch = redis.call('GET', epoch_key)
--if not epoch then
--    epoch = 1
--    redis.call('SET', epoch_key, epoch)
--end
--
---- 1. 遍历校验所有商品的库存是否充足
--for i = 1, item_count do
--    local stock = tonumber(redis.call('GET', KEYS[i]))
--    local demand = tonumber(ARGV[i])
--    if stock == nil or stock < demand then
--        return i
--    end
--end
--
---- 2. 批量扣减库存
--for i = 1, item_count do
--    local demand = tonumber(ARGV[i])
--    redis.call('DECRBY', KEYS[i], demand)
--end
--
---- 3. 生成版本号并写入 outbox payload
--local seq = redis.call('INCR', seq_key)
--local version = tostring(epoch) .. '|' .. tostring(seq)
--local enriched_payload = cjson.decode(payload)
--enriched_payload['epoch'] = tonumber(epoch)
--enriched_payload['seq'] = tonumber(seq)
--enriched_payload['version'] = version
--enriched_payload['source'] = 'REDIS'
--redis.call('HSET', outbox_key, order_id, cjson.encode(enriched_payload))
--
---- 4. 成功返回 0 表示全部扣减成功且消息落库成功
--return 0

-- batch_deduct_stock.lua
-- KEYS: KEYS[1..n] 库存key, KEYS[n+1] outbox key, KEYS[n+2] epoch key,
--       KEYS[n+3] seq key, KEYS[n+4] flag key（新增）
-- ARGV: ARGV[1..n] 扣减数量, ARGV[n+1] orderId, ARGV[n+2] 消息JSON
-- 返回值约定（应用层必须遵守）：
--   0          → 成功（含幂等放行）
--  -99         → 全局 epoch key 丢失，Redis Cluster failover 未完成，拒单保护
--  -(i)  i>0   → 第 i 个商品的库存 key 不存在（Redis 未预热），应用层懒加载后单次重试
--  +(i)  i>0   → 第 i 个商品库存真实不足，直接拒单

local item_count = #KEYS - 4  -- 原来是3，现在改为4
local outbox_key = KEYS[item_count + 1]
local epoch_key  = KEYS[item_count + 2]
local seq_key    = KEYS[item_count + 3]
local flag_key   = KEYS[item_count + 4]  -- 新增：RocketMQ反查凭证
local order_id   = ARGV[item_count + 1]
local payload    = ARGV[item_count + 2]

-- 0. 订单维度幂等：重复orderId直接放行
if redis.call('HEXISTS', outbox_key, order_id) == 1 then
    return 0
end

-- 1. 全局 epoch 检查
--    epoch key 丢失 = Redis Cluster failover 后 EpochInitializer 还未写入
--    返回 -99，应用层拒单并告警，等待服务重启完成 epoch 修复
local epoch = redis.call('GET', epoch_key)
if not epoch then
    -- ❌ 原来：自愈写 1，掩盖了 failover
    -- epoch = 1
    -- redis.call('SET', epoch_key, epoch)

    -- ✅ 现在：返回特殊错误码，由应用层触发 EpochInitializer 重新修复
    return -99  -- EPOCH_MISSING：应用层收到后应告警 + 拒绝下单，等待 epoch 恢复
end


-- 2. 校验所有商品库存是否充足
--    两种失败严格区分，让应用层做不同处理：
--    a. stock == nil  → 库存 key 根本不存在（未预热），返回 -(i) 触发懒加载
--    b. stock < demand → 库存真实不足，返回 +(i) 直接拒单
for i = 1, item_count do
    local stock  = tonumber(redis.call('GET', KEYS[i]))
    local demand = tonumber(ARGV[i])
    --if stock == nil or stock < demand then
    --    return i
    --end
    if stock == nil then
        return -i          -- 负数：key 缺失，应用层懒加载后重试
    end
    if stock < demand then
        return i           -- 正数：库存真实不足，直接 ROLLBACK
    end
end

-- 3. 批量扣减库存
for i = 1, item_count do
    redis.call('DECRBY', KEYS[i], tonumber(ARGV[i]))
end

-- 4. 生成版本号并写入outbox
local seq = redis.call('INCR', seq_key)
local version = tostring(epoch) .. '|' .. tostring(seq)
local enriched_payload = cjson.decode(payload)
enriched_payload['epoch']   = tonumber(epoch)
enriched_payload['seq']     = tonumber(seq)
enriched_payload['version'] = version
enriched_payload['source']  = 'REDIS'
redis.call('HSET', outbox_key, order_id, cjson.encode(enriched_payload))

-- 5. 【新增】原子写入RocketMQ反查凭证，TTL=1小时（覆盖Broker默认反查窗口60秒一次，总共默认15次）
--    flag_key 和库存 key 同在 {stock} hash tag 下，保证原子性：
--    flag 存在 ↔ 库存已扣；主从切换一起丢时两边状态一致，安全 ROLLBACK
redis.call('SET', flag_key, '1', 'EX', 3600)

-- 6. 成功返回0
return 0