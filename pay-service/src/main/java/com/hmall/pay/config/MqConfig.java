package com.hmall.pay.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class MqConfig { //发送者确认机制方法一：return callback
    private final RabbitTemplate rabbitTemplate;

    @PostConstruct //该注解表示：在RabbitTemplate自动装配完以后才会调用该函数
    public void init(){
        rabbitTemplate.setReturnsCallback(new RabbitTemplate.ReturnsCallback() {
            @Override
            public void returnedMessage(ReturnedMessage returnedMessage) {
                log.error("监听到了消息return callback");
                log.debug("exchange：{}",returnedMessage.getExchange());
                log.debug("routingKey：{}",returnedMessage.getRoutingKey());
                log.debug("message：{}",returnedMessage.getMessage());
                log.debug("replyCode：{}",returnedMessage.getReplyCode());
                log.debug("replyText：{}",returnedMessage.getReplyText());
            }
        });
    }
}
