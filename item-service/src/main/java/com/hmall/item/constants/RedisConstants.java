package com.hmall.item.constants;

/**
 * Redis 库存版本与幂等 key 常量。
 * 统一按照《项目执行流程.md》中的命名规范执行。
 */
public interface RedisConstants {

    /**
     * 库存值 key：item:stock:{stock}:itemId
     */
    String ITEM_STOCK_KEY_PREFIX = "item:stock:{stock}:";

    /**
     * LUA版本纪元号key
     */
    String LUA_EPOCH = "item:stock:epoch:{stock}";
    /**
     * LUA版本流水号key
     */
    String ITEM_STOCK_SEQ_KEY_PREFIX = "item:stock:seq:{stock}:";
    //String LUA_SEQUENCE = "item:stock:seq:{stock}";

    /**
     * 库存版本 key：item:stock:ver:{stock}:itemId
     */
    String STOCK_VERSION_KEY_PREFIX = "item:stock:ver:{stock}:";

    /**
     * Redis 集群心跳探针 key，TTL = 15s，由 RedisHeartbeatRefresher 每 5 秒刷新。
     * EpochInitializer 和 RedisFailoverDetector 启动时检测此 key 是否存在，
     * 缺失则保守判定为 failover（主节点宕机期间写丢失）。
     */
    String REDIS_HEARTBEAT_KEY = "item:stock:heartbeat:{stock}";

    // ─────────────────────────────────────────────────────────
    // Failover 分布式协调 key（由 RedisFailoverDetector 管理）
    // ─────────────────────────────────────────────────────────

    /**
     * Failover 升级分布式锁 key，TTL = 45s。
     * 同一时刻只允许一个实例执行 epoch 自增，防止多实例并发重复升级。
     */
    //String FAILOVER_LOCK_KEY = "item:stock:epoch:{stock}:failover_lock";

    /**
     * Failover 已完成标记 key，TTL = lockTTL * 2 = 90s。
     * 抢到锁的实例成功升级后写入此 key，
     * 后续实例看到此 key 存在直接跳过，彻底杜绝重复自增。
     */
    String FAILOVER_DONE_KEY = "item:stock:epoch:{stock}:failover_done";

    /**
     * 库存对账标记 key：item:stock:reconcile:{stock}:itemId
     */
    String STOCK_RECONCILE_KEY_PREFIX = "item:stock:reconcile:{stock}:";

    /**
     * RocketMQ 事务反查凭证 key，TTL = 24h。
     * flag 存在 ↔ 库存已扣；主从切换一起丢时两边状态一致 → 安全 ROLLBACK。
     * 格式：order:flag:{stock}:{orderId}
     */
    String LUA_ORDER_FLAG_PREFIX = "order:flag:{stock}:";

    /** 版本号分隔符，格式：epoch|seq */
    String VERSION_SEPARATOR = "|";
}
