package com.hmall.trade.config;

import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RocketMQTemplateConfig {

    /**
     * 下单主链路：已有的 template，producer group = trade-order-producer-group
     * Spring Boot Starter 自动配置的默认 RocketMQTemplate 就是这个，无需在这里声明。
     */

    /**
     * 取消订单链路专用 template，必须用独立的 producer group，
     * 因为每个 producer group 只能绑定一个 @RocketMQTransactionListener。
     */
    @Bean(name = "CancelRocketMQTemplate")
    public RocketMQTemplate CancelRocketMQTemplate() {
        RocketMQTemplate template = new RocketMQTemplate();
        // producer group 与 @RocketMQTransactionListener 的 rocketMQTemplateBeanName 对应
        template.getProducer().setProducerGroup("trade-cancel-producer-group");
        return template;
    }
}