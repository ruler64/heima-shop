package com.hmall.item.constants;

public interface MQConstants {
    String LAZY_EXCHANGE_NAME = "item.lazy.direct";
    String LAZY_ITEM_ADD_QUEUE_NAME = "item.lazy.es.add.queue";
    String LAZY_ITEM_UPDATE_QUEUE_NAME = "item.lazy.es.update.queue";
    String LAZY_ITEM_DELETE_QUEUE_NAME = "item.lazy.es.delete.queue";
    String LAZY_ITEM_ADD_KEY = "lazy.item.add";
    String LAZY_ITEM_UPDATE_KEY = "lazy.item.update";
    String LAZY_ITEM_DELETE_KEY = "lazy.item.delete";

    String ITEM_CANAL_CHANGE_EXCHANGE = "canal.exchange";
    String ITEM_CANAL_CHANGE_QUEUE = "canal.item.sync.queue";
    String ITEM_CANAL_CHANGE_KEY = "canal.update.item";

    /**
     * 商品服务不再提供 RPC 接口供下单实时调用，而是通过监听消息来扣减库存
     */
//    String ORDER_ITEM_DEDUCT_QUEUE = "order.item.deduct.queue";
//    String ORDER_EVENT_EXCHANGE = "order.event.exchange";
//    String ORDER_ITEM_DEDUCT_KEY = "order.item.deduct.key";

    // 订单取消的交换机和队列（用于逆向补偿）
    String CANCEL_ORDER_EXCHANGE = "trade.cancel.topic";
    String CANCEL_ORDER_QUEUE = "trade.cancel.order.queue";
    String CANCEL_ORDER_KEY = "order.cancel";


    // 订单创建的交换机 (使用 Topic 模式，方便多个微服务订阅)
    String ORDER_EVENT_EXCHANGE = "trade.order.topic";

    // 商品服务监听的队列
    String ORDER_ITEM_DEDUCT_QUEUE = "item.order.created.queue";
    String ORDER_ITEM_DEDUCT_KEY = "order.created";

    /**
     * 商品库存恢复队列 (逆向流程)
     */
    String RESTORE_ITEM_EXCHANGE = "item.restore.topic";
    String RESTORE_ITEM_QUEUE = "item.restore.queue";
    String RESTORE_ITEM_KEY = "item.stock.restore";

    /**
     * 设置最大重试次数
     */
    Integer MAX_RETRY_TIMES = 3;

    /**
     * rocketMQ常量
     * 取消订单与恢复库存半事务消息的topic与消费者组
     */
    public static final String ROCKETMQ_CANCEL_TOPIC = "TRADE_CANCEL_TOPIC";
    public static final String ROCKETMQ_CANCEL_CONSUMER_GROUP = "trade-cancel-consumer-group";
}
