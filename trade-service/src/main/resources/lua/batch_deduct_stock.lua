-- batch_deduct_stock.lua
-- KEYS: KEYS[1..n] 库存 key, KEYS[n+1] Redis outbox key, KEYS[n+2] epoch key, KEYS[n+3] seq key
-- ARGV: ARGV[1..n] 扣减数量, ARGV[n+1] orderId, ARGV[n+2] 序列化消息 JSON

local item_count = #KEYS - 3
local outbox_key = KEYS[item_count + 1]
local epoch_key = KEYS[item_count + 2]
local seq_key = KEYS[item_count + 3]
local order_id = ARGV[item_count + 1]
local payload = ARGV[item_count + 2]

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

-- 2. 先生成一次订单级版本号。seq 只在 Lua 成功路径递增，代表 Redis 预扣减事实顺序。
local seq = redis.call('INCR', seq_key)
local version = tostring(epoch) .. '|' .. tostring(seq)

-- 3. 批量扣减库存，并给每个商品写入本次预扣减后的版本标记
for i = 1, item_count do
    local demand = tonumber(ARGV[i])
    redis.call('DECRBY', KEYS[i], demand)
    redis.call('SET', string.gsub(KEYS[i], 'item:stock:{stock}:', 'item:stock:ver:{stock}:'), version)
end

-- 4. 生成增强消息并写入 Redis outbox。Java 层后续会读取这个增强后的 payload 写 MySQL outbox / 发 MQ。
local enriched_payload = cjson.decode(payload)
enriched_payload['epoch'] = tonumber(epoch)
enriched_payload['seq'] = tonumber(seq)
enriched_payload['version'] = version
enriched_payload['source'] = 'REDIS'
redis.call('HSET', outbox_key, order_id, cjson.encode(enriched_payload))

-- 5. 成功返回 0 表示全部扣减成功且消息落库成功
return 0
