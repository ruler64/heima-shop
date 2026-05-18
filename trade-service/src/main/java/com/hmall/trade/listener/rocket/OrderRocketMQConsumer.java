package com.hmall.trade.listener.rocket;

import com.alibaba.fastjson.JSON;
import com.hmall.common.utils.RabbitMqHelper;
import com.hmall.trade.constants.MQConstants;
import com.hmall.trade.constants.RedisConstants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
@RocketMQMessageListener(
        topic = MQConstants.ROCKETMQ_ORDER_TOPIC,
        consumerGroup = MQConstants.ROCKETMQ_ORDER_CONSUMER_GROUP,
        // 对应RabbitMQ的 concurrency = "16-64"
        consumeThreadNumber = 8,        // 消费线程数（固定值，RocketMQ不支持动态扩缩）
        // 消息拉取间隔
        consumeTimeout = 15000L,         // 消费超时15秒，超时视为失败触发重试
        // 最大重试次数，超过后进入死信Topic（%DLQ%trade-order-consumer-group）
        maxReconsumeTimes = 16
)
@RequiredArgsConstructor
public class OrderRocketMQConsumer implements RocketMQListener<String> {

    private final RabbitMqHelper rabbitMqHelper;
    private final StringRedisTemplate stringRedisTemplate;

    @Override
    public void onMessage(String msgJson) {
        Map<String, Object> msg = JSON.parseObject(msgJson, Map.class);
        String orderId = String.valueOf(msg.get("orderId"));

        log.info("[RocketMQ→RabbitMQ] 收到已提交的下单消息，转发至RabbitMQ。orderId={}", orderId);

        try {
            // 转发到原有RabbitMQ链路，OrderMQListener保持完全不变
            rabbitMqHelper.sendMessageWithConfirm(
                    MQConstants.ASYNC_ORDER_EXCHANGE,
                    MQConstants.ASYNC_ORDER_KEY,
                    msg,
                    MQConstants.MAX_RETRY_TIMES
            );
            log.info("[RocketMQ→RabbitMQ] 转发成功。orderId={}", orderId);
            // ✅ 消费成功后清理 outbox 条目，防止 Hash 无限膨胀
            // 与 Lua 脚本保持一致
            // 要用 buildOutboxKey 算出实际的 key
            long bucket = Math.floorMod(Long.parseLong(orderId), RedisConstants.OUTBOX_BUCKETS);
            String outboxKey = RedisConstants.OUTBOX_KEY_PREFIX + bucket;
            stringRedisTemplate.opsForHash().delete(outboxKey, orderId);
            log.info("[outbox清理] 已删除 outbox 条目。orderId={}", orderId);

        } catch (Exception e) {
            // 抛出异常触发RocketMQ重试（阶梯间隔：10s/30s/1min/2min...）
            // 抛出异常 = NACK + requeue，等价于 channel.basicNack(tag, false, true)
            log.error("[RocketMQ→RabbitMQ] 转发失败，触发RocketMQ重试。orderId={}", orderId, e);
            throw new RuntimeException("转发RabbitMQ失败", e);
        }
    }
}