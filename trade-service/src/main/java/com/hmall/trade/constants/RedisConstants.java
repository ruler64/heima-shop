package com.hmall.trade.constants;

public interface RedisConstants {

    /**
     * 用于做幂等校验的key的前缀，拼上哈希16后的orderId作为key
     */
    //String OUTBOX_KEY_PREFIX = "trade:local_msg_outbox:{stock}:";

    /**
     * 用于做幂等校验的key的前缀，拼上哈希16后的orderId作为key
     */
    // ✅ 新增：每订单独立幂等 key，TTL=24h 自动清理
    String ORDER_IDEM_KEY_PREFIX = "trade:order:idem:{stock}:";

    /**
     * Redis outbox：分桶降低单 Key 大 Hash 的体积与热点
     * 默认Hash分桶数量为16
     */
    //int OUTBOX_BUCKETS = 16;

    /**
     * LUA全局版本纪元号key
     */
    String LUA_EPOCH = "item:stock:epoch:{stock}";
    /**
     * LUA版本per item流水号key
     */
    String LUA_SEQUENCE = "item:stock:seq:{stock}:";
    /**
     * LUA用于RocketMQ反查凭证，TTL=24小时；
     * flag_key 和库存 key 同在 {stock} hash tag 下，保证原子性：
     * flag 存在 ↔ 库存已扣；主从切换一起丢时两边状态一致，安全 ROLLBACK
     */
    String LUA_ORDER_FLAG_PREFIX = "order:flag:{stock}:";


    /**
     * NOTE: Redis Cluster Lua 脚本多 key 运行要求所有 KEYS 落在同一个 hash slot。
     * 因此这里在 key 里使用统一 hash tag：{stock}。
     */
    String ITEM_STOCK_KEY_PREFIX = "item:stock:{stock}:";

    /** 每个商品的版本号key前缀"epoch|seq" */
    String ITEM_STOCK_VERSION_KEY_PREFIX = "item:stock:ver:{stock}:";

}
