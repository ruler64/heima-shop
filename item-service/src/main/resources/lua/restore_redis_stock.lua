-- restore_redis_stock.lua
-- KEYS[1]      = flag_key (order:flag:{stock}:{orderId})
-- KEYS[2..n+1] = stock_keys
-- ARGV[1..n]   = restore quantities
-- 返回: 1=已恢复, 0=跳过(flag不存在，幂等放行)

-- 幂等检查：flag不存在说明已恢复过或从未预扣
if redis.call('EXISTS', KEYS[1]) == 0 then
    return 0
end

-- 恢复各商品库存（仅当stock key存在时才恢复，防止创建脏数据）
for i = 2, #KEYS do
    if redis.call('EXISTS', KEYS[i]) == 1 then
        redis.call('INCRBY', KEYS[i], tonumber(ARGV[i - 1]))
    end
end

-- 删除flag，标记已恢复，防止重复执行
redis.call('DEL', KEYS[1])
return 1