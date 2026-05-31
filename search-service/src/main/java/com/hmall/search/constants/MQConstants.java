package com.hmall.search.constants;

public interface MQConstants {
    String LAZY_EXCHANGE_NAME = "item.lazy.direct";
    String LAZY_ITEM_ADD_QUEUE_NAME = "item.lazy.es.add.queue";
    String LAZY_ITEM_UPDATE_QUEUE_NAME = "item.lazy.es.update.queue";
    String LAZY_ITEM_DELETE_QUEUE_NAME = "item.lazy.es.delete.queue";
    String LAZY_ITEM_ADD_KEY = "lazy.item.add";
    String LAZY_ITEM_UPDATE_KEY = "lazy.item.update";
    String LAZY_ITEM_DELETE_KEY = "lazy.item.delete";

    /** Canal 全局 Topic 交换机（canal.properties 中已配置） */
    String CANAL_EXCHANGE = "canal.exchange";

    /**
     * Order Canal 同步 ES 队列
     * 绑定 routing key：canal.order.change
     * 匹配 order 表和 order_detail 表的 binlog 事件
     */
    String ORDER_CANAL_SYNC_ES_QUEUE = "order.canal.sync.es.queue";
    String ORDER_CANAL_CHANGE_KEY    = "canal.order.change";

    /**
     * Redis 短暂缓存：orderId → userId
     * 用于 order_detail INSERT 时获取 routing 所需的 userId
     * key 格式：search:order:uid:{orderId}
     * TTL：2h（覆盖下单到明细写完的最大窗口）
     */
    String ORDER_UID_CACHE_PREFIX = "search:order:uid:";
    long   ORDER_UID_CACHE_TTL_SECONDS = 7200L;
}
