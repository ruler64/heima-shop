package com.hmall.trade.task;

import com.alibaba.fastjson.JSON;
import com.hmall.common.utils.RabbitMqHelper;
import com.hmall.trade.constants.MQConstants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 该类已不再使用！！！：MQ 消息定时补偿发送任务 (Redis Outbox Pattern 补偿器)
 */
/*
Spring 写法：10 台机器的闹钟会同时响，结果 10 台机器都去 Redis 里抓那几条积压消息往 MQ 发。虽然你有幂等性，但这极大地浪费了 CPU 和带宽。
XXL-JOB 写法：它有一个“调度中心”。闹钟响时，调度中心会只点名其中一台机器去干活。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MqCompensationSpring {//

    private final StringRedisTemplate stringRedisTemplate;
    private final RabbitMqHelper rabbitMqHelper;

    /**
     * 每 10 秒执行一次补偿扫表任务，会产生 “惊群效应”
     * (大厂一般会用 XXL-JOB 分片执行，这里演示 Spring 定时任务)
     */
    @Scheduled(cron = "0/10 * * * * ?")
    public void compensatePendingMessages() {
        String outboxKey = "trade:local_msg_outbox";

        // 1. 获取 Redis Hash 中所有积压的MQ待发消息
        Map<Object, Object> pendingMessages = stringRedisTemplate.opsForHash().entries(outboxKey);

        if (pendingMessages == null || pendingMessages.isEmpty()) {
            return;
        }

        log.info("MQ补偿任务启动: 发现 {} 条因宕机/网络波动滞留的订单消息，开始重试投递...", pendingMessages.size());

        for (Map.Entry<Object, Object> entry : pendingMessages.entrySet()) {
            String orderIdStr = (String) entry.getKey();
            String msgJson = (String) entry.getValue();

            try {
                // 反序列化还原成完整的 msg Map
                Map<String, Object> msg = JSON.parseObject(msgJson, Map.class);

                // 2. 重新投递到 MQ
                rabbitMqHelper.sendMessageWithConfirm(
                        MQConstants.ASYNC_ORDER_EXCHANGE,
                        MQConstants.ASYNC_ORDER_KEY,
                        msg,
                        MQConstants.MAX_RETRY_TIMES
                );

                // 3. 投递成功后，从 Redis 中彻底抹除这条暂存记录
                stringRedisTemplate.opsForHash().delete(outboxKey, orderIdStr);
                log.info("订单 {} 补偿投递 MQ 成功，积压清理完毕。", orderIdStr);

            } catch (Exception e) {
                // 如果依然投递失败，留给下一个 10 秒周期继续努力，直到 RabbitMQ 恢复正常
                log.error("订单 {} 补偿投递 MQ 依然失败，下次定时任务继续重试！", orderIdStr, e);
            }
        }
    }
}