package com.hmall.item.constants;

/**
 * Redis 库存版本与幂等 key 常量。
 * 统一按照《项目执行流程.md》中的命名规范执行。
 */
public interface StockVersionConstants {

    /**
     * 库存值 key：item:stock:{stock}:itemId
     */
    String STOCK_KEY_PREFIX = "item:stock:{stock}:";

    /**
     * 库存版本 key：item:stock:ver:{stock}:itemId
     */
    String STOCK_VERSION_KEY_PREFIX = "item:stock:ver:{stock}:";

    /**
     * 库存幂等 key：item:stock:op:{stock}:orderId:itemId
     */
    String STOCK_OP_KEY_PREFIX = "item:stock:op:{stock}:";

    /**
     * 库存对账标记 key：item:stock:reconcile:{stock}:itemId
     */
    String STOCK_RECONCILE_KEY_PREFIX = "item:stock:reconcile:{stock}:";

    // Redis 版本号格式：epoch|seq
    String VERSION_SEPARATOR = "|";
}
