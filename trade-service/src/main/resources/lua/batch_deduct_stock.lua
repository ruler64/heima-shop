-- batch_deduct_stock.lua
-- KEYS: KEYS[1..n] 库存 key, KEYS[n+1] Redis outbox key, KEYS[n+2] epoch key, KEYS[n+3] seq key
-- ARGV: ARGV[1..n] 扣减数量, ARGV[n+1] orderId, ARGV[n+2] 序列化消息 JSON

local item_count = #KEYS - 3
local outbox_key = KEYS[item_count + 1]
local epoch_key = KEYS[item_count + 2]
local seq_key = KEYS[item_count + 3]
local order_id = ARGV[item_count + 1]
local payload = ARGV[item_count + 2]

-- 0. 订单维度幂等：重复 orderId 直接放行，避免重复预扣 Redis 库存
if redis.call('HEXISTS', outbox_key, order_id) == 1 then
    return 0
end

local epoch = redis.call('GET', epoch_key)
if not epoch then
    epoch = 1
    redis.call('SET', epoch_key, epoch)
end

-- 1. 遍历校验所有商品的库存是否充足
for i = 1, item_count do
    local stock = tonumber(redis.call('GET', KEYS[i]))
    local demand = tonumber(ARGV[i])
    if stock == nil or stock < demand then
        return i
    end
end

-- 2. 批量扣减库存
for i = 1, item_count do
    local demand = tonumber(ARGV[i])
    redis.call('DECRBY', KEYS[i], demand)
end

-- 3. 生成版本号并写入 outbox payload
local seq = redis.call('INCR', seq_key)
local version = tostring(epoch) .. '|' .. tostring(seq)
local enriched_payload = cjson.decode(payload)
enriched_payload['epoch'] = tonumber(epoch)
enriched_payload['seq'] = tonumber(seq)
enriched_payload['version'] = version
enriched_payload['source'] = 'REDIS'
redis.call('HSET', outbox_key, order_id, cjson.encode(enriched_payload))

-- 4. 成功返回 0 表示全部扣减成功且消息落库成功
return 0