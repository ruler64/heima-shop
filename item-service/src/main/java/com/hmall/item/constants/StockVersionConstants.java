package com.hmall.item.constants;

/**
 * 方案 B：Redis 库存版本与幂等 key 常量。
 */
public interface StockVersionConstants {

    String STOCK_KEY_PREFIX = "item:stock:";
    String STOCK_VERSION_KEY_PREFIX = "item:stock:ver:";
    String STOCK_OP_KEY_PREFIX = "item:stock:op:";
    String STOCK_RECONCILE_KEY_PREFIX = "item:stock:reconcile:";

    // Redis 版本号格式：epoch|seq
    String VERSION_SEPARATOR = "|";
}
