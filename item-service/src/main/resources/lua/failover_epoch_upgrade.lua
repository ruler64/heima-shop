-- failover_epoch_upgrade.lua
--
-- 职责：Redis Cluster Failover 后，原子地将全局 epoch 自增一次。
-- 保证同一次 Failover 事件只触发一次 epoch 自增，多实例并发安全。
--
-- KEYS[1]  epochKey  : 全局纪元 key
-- KEYS[2]  doneKey   : 本次 Failover 已处理标记 key
--
-- ARGV[1]  lockTTL   : 锁的超时秒数（建议 45s）
-- ARGV[2]  initEpoch : epoch key 不存在时的初始化值（由 Java 侧从 MySQL 读取后传入）
--
-- 返回值约定：
--   0        → doneKey 存在或未抢到锁，本实例无需处理（其他实例已处理完毕）
--   正整数   → 本实例抢到锁并成功将 epoch 升级 / 初始化到该值
--   -1       → 不应出现（保留，用于未来扩展）
--
-- ─────────────────────────────────────────────────────────────────────────────
-- 防重复自增核心机制：
--   Step1  检测 doneKey：若存在说明同一 Failover 事件已被处理，直接返回 0
--   Step3a epochKey 存在  → INCR，写入 doneKey（TTL = lockTTL*2）
--   Step3b epochKey 不存在→ SET 初始值，写入 doneKey（TTL = lockTTL*2）
--   返回最新 epoch 值
-- ─────────────────────────────────────────────────────────────────────────────

--local lockKey   = KEYS[1]
local epochKey  = KEYS[1]
local doneKey   = KEYS[2]
local lockTTL   = tonumber(ARGV[1])
local initEpoch = tonumber(ARGV[2])

-- Step1：doneKey 存在 → 本次 Failover 已被其他实例处理，直接放行
if redis.call('EXISTS', doneKey) == 1 then
    return 0
end

-- Step2：抢分布式锁
--local locked = redis.call('SET', lockKey, '1', 'NX', 'EX', lockTTL)
--if not locked then
--    -- 未抢到锁，说明另一个实例正在执行，返回 0
--    return 0
--end

-- Step3：执行 epoch 升级
local newEpoch
if redis.call('EXISTS', epochKey) == 1 then
    -- epochKey 存在（Failover 后从节点晋升，从节点上可能还有旧 epoch）
    -- 原子自增，确保全局 epoch 单调递增
    newEpoch = redis.call('INCR', epochKey)
else
    -- epochKey 完全消失（全量数据丢失 / 首次部署）
    -- 使用 Java 侧从 MySQL 读取的安全初始值
    redis.call('SET', epochKey, tostring(initEpoch))
    newEpoch = initEpoch
end

-- Step4：写入 "本次 Failover 已处理" 标记
-- TTL = lockTTL * 2，覆盖锁的两个周期，杜绝锁过期后其他实例重复自增
redis.call('SET', doneKey, '1', 'EX', lockTTL * 2)

return newEpoch