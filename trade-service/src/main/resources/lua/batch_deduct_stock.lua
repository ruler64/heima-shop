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
--
-- KEYS结构（总共 3n+3 个）:
-- KEYS[1..n]          库存key: item:stock:{stock}:{itemId}
-- KEYS[n+1]           idem_key: trade:order:idem:{stock}:{orderId}
-- KEYS[n+2]           epoch_key: item:stock:epoch:{stock}
-- KEYS[n+3]           flag_key: order:flag:{stock}:{orderId}
-- KEYS[n+4..2n+3]     per-item seq key: item:stock:seq:{stock}:{itemId}
-- KEYS[2n+4..3n+3]    per-item ver key: item:stock:ver:{stock}:{itemId}
--
-- ARGV结构（总共 n+1 个）:
-- ARGV[1..n]          扣减数量
-- ARGV[n+1]           orderId
--
-- 返回值（List<Long>）:
--   {0}       → 成功（含幂等放行）
--   {-99}     → epoch丢失，拒单
--   {-i,...}  → 第i个商品库存key缺失（返回所有缺失集合）
--   {i}       → 第i个商品库存真实不足

local item_count = math.floor((#KEYS - 3) / 3)
local idem_key  = KEYS[item_count + 1]
local epoch_key = KEYS[item_count + 2]
local flag_key  = KEYS[item_count + 3]
local order_id  = ARGV[item_count + 1]

-- 0. 幂等检查
if redis.call('EXISTS', idem_key) == 1 then
    return {0}
end

-- 1. epoch检查（缺失=failover未完成，拒单保护）
local epoch = redis.call('GET', epoch_key)
if not epoch then
    return {-99}
end

-- 2. 收集所有缺失的库存key（全量返回，触发批量懒加载）
local missing_indices = {}
for i = 1, item_count do
    if redis.call('EXISTS', KEYS[i]) == 0 then
        table.insert(missing_indices, -i)
    end
end
if #missing_indices > 0 then
    return missing_indices
end

-- 3. 校验库存充足性（先全量校验再扣减，防止部分成功）
for i = 1, item_count do
    local stock  = tonumber(redis.call('GET', KEYS[i]))
    local demand = tonumber(ARGV[i])
    if stock < demand then
        return {i}
    end
end

-- 4. 批量扣减库存 + 更新每商品独立版本号（原子完成）
for i = 1, item_count do
    redis.call('DECRBY', KEYS[i], tonumber(ARGV[i]))

    local seq_key = KEYS[item_count + 3 + i]        -- per-item seq key
    local ver_key = KEYS[2 * item_count + 3 + i]    -- per-item ver key
    local seq = redis.call('INCR', seq_key)          -- per-item 自增，互不干扰
    redis.call('SET', ver_key, tostring(epoch) .. '|' .. tostring(seq))
end

-- 5. 写幂等key（24h TTL）和RocketMQ反查凭证
redis.call('SET', idem_key, '1', 'EX', 86400)
redis.call('SET', flag_key, '1', 'EX', 86400)

return {0}


-- batch_deduct_stock.lua

-- KEYS[1..n]      库存key
-- KEYS[n+1]       idem_key
-- KEYS[n+2]       epoch_key
-- KEYS[n+3]       seq_key
-- KEYS[n+4]       flag_key
-- KEYS[n+5..2n+4] 版本key（新增）
-- ARGV[1..n]      扣减数量
-- ARGV[n+1]       orderId

-- 返回值（统一返回 array，Java侧用 List<Long> 接收）：
--   {0}       → 成功
--   {-99}     → epoch丢失
--   {-i,...}  → 第i个商品库存key缺失（所有缺失的集合）
--   {i}       → 第i个商品库存真实不足

--local item_count = math.floor((#KEYS - 4) / 2)  -- ← 改为除以2，因为新增了n个版本key,返回浮点，用 math.floor 明确取整
--local idem_key  = KEYS[item_count + 1]
--local epoch_key = KEYS[item_count + 2]
--local seq_key   = KEYS[item_count + 3]
--local flag_key  = KEYS[item_count + 4]
--local order_id  = ARGV[item_count + 1]
--
---- 0. 幂等检查
--if redis.call('EXISTS', idem_key) == 1 then
--    return {0}
--end
--
---- 1. epoch检查
--local epoch = redis.call('GET', epoch_key)
--if not epoch then
--    return {-99}
--end
--
---- 2. 检查所有缺失的库存key（收集全部，不再只返回第一个）
--local missing_indices = {}
--for i = 1, item_count do
--    if redis.call('EXISTS', KEYS[i]) == 0 then
--        table.insert(missing_indices, -i)  -- 负数表示key缺失
--    end
--end
--
--if #missing_indices > 0 then
--    return missing_indices  -- 返回所有缺失商品的index集合
--end
--
---- 3. 校验库存充足性
--for i = 1, item_count do
--    local stock  = tonumber(redis.call('GET', KEYS[i]))
--    local demand = tonumber(ARGV[i])
--    if stock < demand then
--        return {i}  -- 正数表示真实不足
--    end
--end
--
---- 4. 批量扣减库存
--for i = 1, item_count do
--    redis.call('DECRBY', KEYS[i], tonumber(ARGV[i]))
--end
--
---- 5. 生成版本号，更新所有商品的版本key（新增）
--local seq = redis.call('INCR', seq_key)
--local version = tostring(epoch) .. '|' .. tostring(seq)
--for i = 1, item_count do
--    local ver_key = KEYS[item_count + 4 + i]  -- 版本key从n+5开始
--    redis.call('SET', ver_key, version)        -- 无TTL，与预热保持一致
--end
--
---- 6. 写幂等key和RocketMQ反查凭证
--redis.call('SET', idem_key, '1', 'EX', 86400)
--redis.call('SET', flag_key, '1', 'EX', 86400)
--
--return {0}