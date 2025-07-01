package com.hmall.trade.config;

import com.alibaba.druid.sql.visitor.functions.Bin;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.rabbit.retry.RepublishMessageRecoverer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class ErrorMessageConfiguration {
    private final RabbitTemplate rabbitTemplate;

    @Bean
    public DirectExchange errorExchange(){
        return new DirectExchange("error.direct");
    }

    @Bean
    public Queue errorQueue(){
        return QueueBuilder
                .durable("error.queue")
                .lazy() //开启Lazy模式
                .build();
    }

    @Bean
    public Binding errorQueueBinding(Queue errorQueue, DirectExchange errorExchange){
        return BindingBuilder.bind(errorQueue).to(errorExchange).with("error");
    }

    @Bean
    public MessageRecoverer messageRecoverer(){//消费者重试机制失败次数耗尽后，将失败消息投递到失败交换机error.direct然后根据key投递到失败队列error.queue
        return new RepublishMessageRecoverer(rabbitTemplate,"error.direct","error");
    }
}
