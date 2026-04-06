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

    /**
     * 任务名: mqCompensationJob (去 XXL-JOB Admin 界面配置该名称)
     */
    @XxlJob("mqCompensationJob")
    public void compensatePendingMessages() {
        String outboxKey = "trade:local_msg_outbox";

        // XxlJobHelper.log 可以在 XXL-JOB 调度中心页面直接看到执行日志
        XxlJobHelper.log("MQ补偿任务启动: 开始扫描 Redis 待发消息表...");

        // 1. 获取 Redis Hash 中所有积压的待发消息
        Map<Object, Object> pendingMessages = stringRedisTemplate.opsForHash().entries(outboxKey);

        if (pendingMessages == null || pendingMessages.isEmpty()) {
            XxlJobHelper.log("当前无积压的 MQ 消息，任务结束。");
            return; // 顺利执行完毕
        }

        XxlJobHelper.log("发现 {} 条滞留消息，开始重试投递...", pendingMessages.size());

        int successCount = 0;
        int failCount = 0;

        for (Map.Entry<Object, Object> entry : pendingMessages.entrySet()) {
            String orderIdStr = (String) entry.getKey();
            String msgJson = (String) entry.getValue();

            try {
                Map<String, Object> msg = JSON.parseObject(msgJson, Map.class);

                // 2. 重新投递到 MQ
                rabbitMqHelper.sendMessageWithConfirm(
                        MQConstants.ASYNC_ORDER_EXCHANGE,
                        MQConstants.ASYNC_ORDER_KEY,
                        msg,
                        MQConstants.MAX_RETRY_TIMES
                );

                // 3. 投递成功后抹除记录
                stringRedisTemplate.opsForHash().delete(outboxKey, orderIdStr);
                XxlJobHelper.log("订单 {} 补偿投递 MQ 成功", orderIdStr);
                successCount++;

            } catch (Exception e) {
                // 单个消息失败不要抛出异常中断整个循环，记录日志继续处理下一个
                XxlJobHelper.log("订单 {} 补偿投递 MQ 失败，异常信息: {}", orderIdStr, e.getMessage());
                log.error("订单 {} 补偿失败", orderIdStr, e);
                failCount++;
            }
        }

        // 总结汇报
        String resultMsg = String.format("任务执行完成。成功补偿: %d 条，失败: %d 条", successCount, failCount);
        XxlJobHelper.log(resultMsg);

        // 如果有失败的，让 XXL-JOB 的调度状态变红报警
        if (failCount > 0) {
            XxlJobHelper.handleFail(resultMsg);
        } else {
            XxlJobHelper.handleSuccess(resultMsg);
        }
    }
}