-- batch_lazy_load_stock.lua
-- KEYS[1..n]       库存 key  (item:stock:{stock}:{itemId})
-- KEYS[n+1..2n]    版本 key  (item:stock:ver:{stock}:{itemId})
-- KEYS[2n+1..3n]   per item sequence key  (item:stock:seq:{stock}:{itemId})
-- KEYS[3n+1]       epoch key (item:stock:epoch:{stock})
-- ARGV[1..n]       MySQL 库存值
-- 返回: 实际写入的商品数量

local n = (#KEYS - 1) / 3
local epoch_key = KEYS[3 * n + 1]

-- 从 Redis 读取当前全局 epoch，用于初始化版本key
local epoch = redis.call('GET', epoch_key)
local initVersion = epoch and (tostring(epoch) .. '|0') or '1|0'

local written = 0
for i = 1, n do
    local stock_key = KEYS[i]
    local ver_key = KEYS[n + i]
    local seq_key = KEYS[2 * n + i]

    -- NX = setIfAbsent：并发时不覆盖已有值
    local ok = redis.call('SET', stock_key, ARGV[i], 'NX')

    -- 优化：如果库存是新写入的，则一并初始化版本和流水号
    if ok then
        redis.call('SET', ver_key, initVersion, 'NX')
        redis.call('SET', seq_key, '0', 'NX') -- 补全：初始化独立的流水号为 0
        written = written + 1
    end
end

return written