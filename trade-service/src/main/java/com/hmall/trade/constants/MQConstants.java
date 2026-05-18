package com.hmall.trade.constants;

public interface MQConstants {
    /**
     * 延迟队列
     */
    String DELAY_EXCHANGE_NAME = "trade.delay.direct";
    String DELAY_ORDER_QUEUE_NAME = "trade.delay.order.queue";
    String DELAY_ORDER_KEY = "delay.order.query";

    /**
     * direct直连队列
     */
    String ASYNC_ORDER_EXCHANGE = "trade.async.order.direct";
    String ASYNC_ORDER_QUEUE = "trade.async.order.queue";
    String ASYNC_ORDER_KEY = "async.order";

    /**
     * 订单取消的交换机和队列（用于逆向补偿）
     */
    String CANCEL_ORDER_EXCHANGE = "trade.cancel.topic";
    String CANCEL_ORDER_QUEUE = "trade.cancel.order.queue";
    String CANCEL_ORDER_KEY = "order.cancel";


    // 订单创建的交换机 (使用 Topic 模式，方便多个微服务订阅)
    String ORDER_EVENT_EXCHANGE = "trade.order.topic";

    // 商品扣减库存服务监听的队列
    String ORDER_ITEM_DEDUCT_QUEUE = "item.order.created.queue";
    String ORDER_ITEM_DEDUCT_KEY = "order.created";

    // 购物车服务监听的队列
    String ORDER_CART_CLEAR_QUEUE = "cart.order.created.queue";
    String ORDER_CART_CLEAR_KEY = "order.created";
    // 购物车服务实际监听的交换机名（在 cart-service 内部）
    String ORDER_CART_CLEAR_EXCHANGE = "direct.order.cart.direct";

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
     * 下单半事务消息的topic与消费者组
     */
    public static final String ROCKETMQ_ORDER_TOPIC = "TRADE_ORDER_TOPIC";
    public static final String ROCKETMQ_ORDER_CONSUMER_GROUP = "trade-order-consumer-group";
    /**
     * rocketMQ常量
     * 取消订单与恢复库存半事务消息的topic与消费者组
     */
    public static final String ROCKETMQ_CANCEL_TOPIC = "TRADE_CANCEL_TOPIC";
    public static final String ROCKETMQ_CANCEL_CONSUMER_GROUP = "trade-cancel-consumer-group";
}
