-- batch_deduct_stock.lua
-- KEYS: KEYS[1] 到 KEYS[n] 是库存 key(例如: item:stock:1, item:stock:2), KEYS[n+1] 是本地消息表的 hash key
-- ARGV: ARGV[1] 到 ARGV[n] 是扣减数量, ARGV[n+1] 是 orderId, ARGV[n+2] 是序列化后的订单消息 JSON

local item_count = #KEYS - 1
local outbox_key = KEYS[item_count + 1]
local order_id = ARGV[item_count + 1]
local payload = ARGV[item_count + 2]

-- 1. 遍历校验所有商品的库存是否充足//无基于 Redis 的“内存消息表”，会有双写一致性漏洞
--for i, key in ipairs(KEYS) do
--    local stock = tonumber(redis.call('get', key))
--    local demand = tonumber(ARGV[i])
--    if stock == nil or stock < demand then
--        -- 库存不足，直接返回当前处理的失败索引对应的ID（这里可以约定返回特定的错误码或ID）
--        return i
--    end
--end
--
---- 2. 校验全通过，所有库存都充足，执行批量扣减
--for i, key in ipairs(KEYS) do
--    local demand = tonumber(ARGV[i])
--    redis.call('decrby', key, demand)
--end
-- 1. 遍历校验所有商品的库存是否充足，Lua脚本不能回滚只能保证原子性，所以防止中途库存不够导致数据不一致问题
for i=1, item_count do
    local stock = tonumber(redis.call('get', KEYS[i]))
    local demand = tonumber(ARGV[i])
    if stock == nil or stock < demand then
        -- 库存不足，直接返回当前处理的失败索引对应的ID
        return i
    end
end

-- 2. 校验全通过，所有库存都充足，执行批量扣减
for i=1, item_count do
    local demand = tonumber(ARGV[i])
    redis.call('decrby', KEYS[i], demand)
end

-- 3. 【大厂精髓】原子性地将 MQ 待发消息暂存到 Redis 本地消息表！，防止双写中途宕机导致数据不一致问题
redis.call('HSET', outbox_key, order_id, payload)

-- 4. 成功返回 0 表示全部扣减成功且消息落库成功
return 0