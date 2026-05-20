package com.hmall.cart.constants;

public interface MQConstants {
    /**
     * direct直连队列
     */
    String ORDER_EVENT_EXCHANGE = "direct.order.cart.direct";
//    String ORDER_CART_CLEAR_QUEUE = "direct.order.cart.clear.queue";
//    String ORDER_CART_CLEAR_KEY = "direct.order.cart.clear.key";
// 购物车服务监听的队列
    String ORDER_CART_CLEAR_QUEUE = "cart.order.created.queue";
    String ORDER_CART_CLEAR_KEY = "order.created";

    // 订单取消的交换机和队列（用于逆向补偿）
    String CANCEL_ORDER_EXCHANGE = "trade.cancel.topic";
    String CANCEL_ORDER_QUEUE = "trade.cancel.order.queue";
    String CANCEL_ORDER_KEY = "order.cancel";


    /**
     * 设置最大重试次数
     */
    Integer MAX_RETRY_TIMES = 3;

    /**
     * RocketMQ 核心订单落库事务 Topic (一端提交，多端订阅)
     * 与他对应的消费者组
     */
    String ROCKETMQ_DB_ORDER_TOPIC = "TRADE_DB_ORDER_TOPIC";
    String ROCKETMQ_DB_ORDER_PRODUCER_GROUP = "trade-db-order-producer-group";//半事务消息消费者组

    /**
     * 各服务对应的 RocketMQ 消费组名称
     */
    String ROCKETMQ_CART_CLEAR_GROUP = "cart-clear-group";
}
