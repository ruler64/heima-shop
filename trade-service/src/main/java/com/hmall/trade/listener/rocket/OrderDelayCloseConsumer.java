package com.hmall.trade.listener.rocket;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hmall.trade.constants.MQConstants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * 订单落库广播消费者（trade-service）。
 * 负责发送 RocketMQ 延迟关单消息（保留延迟队列，20min 精确延迟）。
 */
@Slf4j
@Service
@RocketMQMessageListener(
        topic = MQConstants.ROCKETMQ_DB_ORDER_TOPIC,
        consumerGroup = MQConstants.ROCKETMQ_ORDER_DELAY_GROUP,
        consumeThreadNumber = 8,
        consumeTimeout = 15000L,
        maxReconsumeTimes = 16
)
@RequiredArgsConstructor
public class OrderDelayCloseConsumer implements RocketMQListener<String> {

    // 延迟关单继续用 RabbitMQ（支持精确 15min 延迟）
    private final RabbitTemplate rabbitTemplate;

    private final RocketMQTemplate rocketMQTemplate;

    @Override
    public void onMessage(String msgJson) {
        Long orderId = JSON.parseObject(msgJson).getLong("orderId");
        log.info("[延迟关单] 收到落库广播，发送 20min 延迟关单。orderId={}", orderId);

        // 构建消息体，携带初始 recheckTimes=0
        Map<String, Object> payload = new HashMap<>();
        payload.put("orderId", orderId);
        payload.put("recheckTimes", 0);

        rocketMQTemplate.syncSend(// 同步发送，阻塞等待
                MQConstants.ROCKETMQ_DELAY_CLOSE_TOPIC,
                MessageBuilder.withPayload(JSON.toJSONString(payload)).build(),
                3000,   // 发送超时时间3秒，超过这个时间就会果断放弃等待，并直接抛出超时异常；此时会去重试消费
                15  // level 15 = 20min
        );
        log.info("[延迟关单] RocketMQ 延迟消息发送成功。orderId={}", orderId);
        /*JSONObject json = JSON.parseObject(msgJson);
        Long orderId = json.getLong("orderId");

        log.info("[延迟关单] 收到落库广播，发送 15min 延迟关单消息。orderId={}", orderId);

        // 发送 RabbitMQ 延迟消息（与原有 OrderDelayMessageListener 对接）
        rabbitTemplate.convertAndSend(
                MQConstants.DELAY_EXCHANGE_NAME,
                MQConstants.DELAY_ORDER_KEY,
                orderId,
                message -> {
                    message.getMessageProperties().setDelay(15 * 60 * 1000);
                    return message;
                }
        );

        log.info("[延迟关单] 15min 延迟消息发送成功。orderId={}", orderId);*/
    }
}