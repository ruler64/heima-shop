package com.hmall.trade.listener.rocket;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RocketMQMessageListener(
        topic = "%DLQ%trade-order-consumer-group",  // 死信Topic固定命名：%DLQ% + 原消费者组名
        consumerGroup = "trade-order-dlq-consumer-group"    //不能和正常消费者组的 trade-order-consumer-group 重名，否则两个消费者组会抢消息
)
@RequiredArgsConstructor
public class OrderDLQConsumer implements RocketMQListener<String> {

    @Override
    public void onMessage(String msgJson) {
        // 进入死信说明重试16次仍失败，需要人工介入
        // 生产环境：发告警（钉钉/企微）+ 写MySQL人工处理表
        log.error("[死信告警] 下单消息多次重试失败，需人工处理！msg={}", msgJson);
        // TODO: 接入告警系统
    }
}
