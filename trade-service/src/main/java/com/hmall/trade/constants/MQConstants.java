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
     * 执行LUA脚本预扣减Redis库存的半事务消息
     */
    String ROCKETMQ_LUA_TOPIC = "LUA_TOPIC";
    String ROCKETMQ_LUA_CONSUMER_GROUP = "lua-consumer-group";
    /**
     * rocketMQ常量
     * 下单半事务消息的topic与消费者组
     */
    String ROCKETMQ_ORDER_TOPIC = "TRADE_ORDER_TOPIC";
    String ROCKETMQ_ORDER_CONSUMER_GROUP = "trade-order-consumer-group";
    /**
     * rocketMQ常量
     * 取消订单与恢复库存半事务消息的topic与消费者组
     */
    String ROCKETMQ_CANCEL_TOPIC = "TRADE_CANCEL_TOPIC";
    String ROCKETMQ_CANCEL_CONSUMER_GROUP = "trade-cancel-consumer-group";

    /**
     * RocketMQ 核心订单落库事务 Topic (一端提交，多端订阅)
     * 与他对应的消费者组
     */
    String ROCKETMQ_DB_ORDER_TOPIC = "TRADE_DB_ORDER_TOPIC";
    String ROCKETMQ_DB_ORDER_PRODUCER_GROUP = "trade-db-order-producer-group";//广播消息消费者组

    /**
     * 各服务对应的 RocketMQ 消费组名称
     */
    String ROCKETMQ_ITEM_DEDUCT_GROUP = "item-stock-deduct-group";
    String ROCKETMQ_CART_CLEAR_GROUP = "cart-clear-group";
    String ROCKETMQ_ORDER_DELAY_GROUP = "trade-order-delay-check-group";

    /**
     * 关单功能：监听延迟消息的topic和消费者 TTL=20min
     */
    String ROCKETMQ_DELAY_CLOSE_TOPIC = "TRADE_DELAY_CLOSE_TOPIC";
    String ROCKETMQ_DELAY_CLOSE_GROUP = "trade-delay-close-consumer-group";

    /**
     * Canal用于监听创建订单事件，毫秒级发送消息给下游消费者；不保证一定成功，只监听一次insert；
     * 如果不成功用xxlJob轮询兜底。这里用topic
     */
    String ORDER_CANAL_RABBITMQ_EXCHANGE = "canal.exchange";
    String ORDER_CANAL_RABBITMQ_QUEUE = "canal.outbox.sync.queue";
    String ORDER_CANAL_RABBITMQ_KEY = "canal.insert.outbox";

    // 新增：Order 同步 ES 专用常量
    public static final String ORDER_CANAL_SYNC_ES_QUEUE = "order.canal.sync.es.queue";
    public static final String ORDER_CANAL_CHANGE_KEY = "canal.order.change";

}
