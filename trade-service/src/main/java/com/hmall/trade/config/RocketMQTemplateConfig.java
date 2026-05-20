package com.hmall.trade.config;

import com.hmall.trade.constants.MQConstants;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RocketMQTemplateConfig {

    @Value("${rocketmq.name-server}")
    private String nameServer;

    /**
     * 下单主链路：已有的 template，producer group = trade-order-producer-group
     * Spring Boot Starter 自动配置的默认 RocketMQTemplate 就是这个，无需在这里声明。
     */
    @Bean(name = "LuaRocketMQTemplate")
    public RocketMQTemplate luaRocketMQTemplate() {
        RocketMQTemplate template = new RocketMQTemplate();
        TransactionMQProducer producer = new TransactionMQProducer(MQConstants.ROCKETMQ_LUA_CONSUMER_GROUP);
        producer.setNamesrvAddr(nameServer);
        template.setProducer(producer);
        return template;
    }

    /**
     * 取消订单链路专用 template，必须用独立的 producer group，
     * 因为每个 producer group 只能绑定一个 @RocketMQTransactionListener。
     */
    @Bean(name = "CancelRocketMQTemplate")
    public RocketMQTemplate cancelRocketMQTemplate() {
        RocketMQTemplate template = new RocketMQTemplate();
        TransactionMQProducer producer = new TransactionMQProducer(MQConstants.ROCKETMQ_CANCEL_CONSUMER_GROUP);
        producer.setNamesrvAddr(nameServer);
        template.setProducer(producer);
        return template;
    }

    /**
     * 订单落库广播专用 template。
     * 独立 producer group，绑定 DbOrderTransactionListener。
     * COMMIT 后广播给 item/cart/delay 三个消费者组。
     */
    @Bean(name = "DbOrderRocketMQTemplate")
    public RocketMQTemplate dbOrderRocketMQTemplate() {
        RocketMQTemplate template = new RocketMQTemplate();
        TransactionMQProducer producer = new TransactionMQProducer(MQConstants.ROCKETMQ_DB_ORDER_PRODUCER_GROUP);
        producer.setNamesrvAddr(nameServer);
        template.setProducer(producer);
        return template;
    }
}