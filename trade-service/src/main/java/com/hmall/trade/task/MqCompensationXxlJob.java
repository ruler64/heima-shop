package com.hmall.trade.task;

import com.alibaba.fastjson.JSON;
import com.hmall.common.utils.RabbitMqHelper;
import com.hmall.trade.constants.MQConstants;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 基于 XXL-JOB 的 MQ 消息兜底补偿任务：MQ 消息定时补偿发送任务 (Redis Outbox Pattern 补偿器)存在redis中
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MqCompensationXxlJob {

    private final StringRedisTemplate stringRedisTemplate;
    private final RabbitMqHelper rabbitMqHelper;

    private static final int OUTBOX_BUCKETS = 16;
    private static final String OUTBOX_KEY_PREFIX = "trade:local_msg_outbox:{stock}:";

    /**
     * 任务名: mqCompensationJob (去 XXL-JOB Admin 界面配置该名称)
     */
    @XxlJob("mqCompensationJob")
    public void compensatePendingMessages() {
        // XxlJobHelper.log 可以在 XXL-JOB 调度中心页面直接看到执行日志
        XxlJobHelper.log("MQ补偿任务启动: 开始扫描 Redis 分桶 Outbox...");

        int successCount = 0;
        int failCount = 0;

        for (int bucket = 0; bucket < OUTBOX_BUCKETS; bucket++) {
            String outboxKey = OUTBOX_KEY_PREFIX + bucket;

            Map<Object, Object> pendingMessages = stringRedisTemplate.opsForHash().entries(outboxKey);
            if (pendingMessages == null || pendingMessages.isEmpty()) {
                continue;
            }

            XxlJobHelper.log("桶 {} 发现 {} 条滞留消息，开始重试投递...", bucket, pendingMessages.size());

            for (Map.Entry<Object, Object> entry : pendingMessages.entrySet()) {
                String orderIdStr = (String) entry.getKey();
                String msgJson = (String) entry.getValue();

                try {
                    Map<String, Object> msg = JSON.parseObject(msgJson, Map.class);


                    rabbitMqHelper.sendMessageWithConfirm(
                            MQConstants.ASYNC_ORDER_EXCHANGE,
                            MQConstants.ASYNC_ORDER_KEY,
                            msg,
                            MQConstants.MAX_RETRY_TIMES
                    );

                    stringRedisTemplate.opsForHash().delete(outboxKey, orderIdStr);
                    XxlJobHelper.log("订单 {} 补偿投递 MQ 成功", orderIdStr);
                    successCount++;

                } catch (Exception e) {
                    XxlJobHelper.log("订单 {} 补偿投递 MQ 失败，异常信息: {}", orderIdStr, e.getMessage());
                    log.error("订单 {} 补偿失败", orderIdStr, e);
                    failCount++;
                }
            }
        }

        String resultMsg = String.format("任务执行完成。成功补偿: %d 条，失败: %d 条", successCount, failCount);
        XxlJobHelper.log(resultMsg);

        // 这里不要因为单条补偿失败就把整次调度判成失败。
        // 失败的消息仍然保留在 Redis 中，交给下一次定时任务继续重试即可。
        XxlJobHelper.handleSuccess(resultMsg);
    }
}