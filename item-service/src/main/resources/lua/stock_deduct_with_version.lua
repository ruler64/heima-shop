-- 方案 B：Redis 库存扣减 + 版本号 + 幂等，原子执行
-- KEYS:
--   1: item:stock:{itemId}
--   2: item:stock:ver:{itemId}
--   3: item:stock:op:{orderId}:{itemId}
-- ARGV:
--   1: opId (orderId:itemId 或 traceId)
--   2: deductNum
--   3: newEpoch
--   4: newSeq
-- 返回值：
--   1 = 成功扣减
--   0 = 幂等命中，已处理过
--  -1 = 库存不足
--  -2 = 版本回退/旧消息

local stockKey = KEYS[1]
local verKey = KEYS[2]
local opKey = KEYS[3]

local opId = ARGV[1]
local deductNum = tonumber(ARGV[2])
local newEpoch = tonumber(ARGV[3])
local newSeq = tonumber(ARGV[4])

local currentStock = tonumber(redis.call('get', stockKey) or '0')
if currentStock < deductNum then
    return -1
end

local verVal = redis.call('get', verKey)
local oldEpoch = -1
local oldSeq = -1
if verVal then
    local sepPos = string.find(verVal, '|')
    if sepPos then
        oldEpoch = tonumber(string.sub(verVal, 1, sepPos - 1)) or -1
        oldSeq = tonumber(string.sub(verVal, sepPos + 1)) or -1
    end
end

if newEpoch < oldEpoch or (newEpoch == oldEpoch and newSeq <= oldSeq) then
    return -2
end

-- 幂等：确认版本可写后，再把 opKey 记入，防止失败消息误占坑
if redis.call('setnx', opKey, opId) == 0 then
    return 0
end
redis.call('expire', opKey, 86400)

redis.call('decrby', stockKey, deductNum)
redis.call('set', verKey, tostring(newEpoch) .. '|' .. tostring(newSeq))
return 1