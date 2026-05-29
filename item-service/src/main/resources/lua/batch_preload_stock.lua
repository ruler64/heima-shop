-- batch_preload_stock.lua
-- 专门用于安全初始化版本和流水号（绝不覆盖已有活跃数据）
-- KEYS[1..n]:   版本 key  (ver_key)
-- KEYS[n+1..2n]: 流水号 key (seq_key)
-- ARGV[1]:      要初始化的版本字符串 (例如 "2|0")

local n = #KEYS / 2
local initVersion = ARGV[1]

for i = 1, n do
    local ver_key = KEYS[i]
    local seq_key = KEYS[n + i]
    
    -- 原子校验：只有当这两个 key 都不存在时，才进行预热初始化
    -- 防止应用启动慢了，已经有订单进来产生了真实的 seq，被预热粗暴覆盖
    if redis.call('EXISTS', seq_key) == 0 then
        redis.call('SET', ver_key, initVersion)
        redis.call('SET', seq_key, '0')
    end
end

return 1