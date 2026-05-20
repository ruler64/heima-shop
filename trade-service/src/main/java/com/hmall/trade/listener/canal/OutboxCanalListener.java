package com.hmall.trade.listener.canal;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hmall.trade.constants.MQConstants;
import com.hmall.trade.mapper.LocalEventOutboxMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * Canal 监听 local_event_outbox 表 INSERT 事件。
 * 替代 XXL-Job 轮询，实时补偿 afterCommit 发送失败的广播消息。
 *
 * 触发时机：handleDbOrder 写入 outbox(status=0) 后，Canal 感知 binlog。
 * 与 afterCommit 的关系：
 *   - afterCommit 成功 → outbox status=1 → Canal 事件到来时跳过（已处理）
 *   - afterCommit 失败/宕机 → outbox status=0 → Canal 触发补偿
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxCanalListener {

    private final RocketMQTemplate rocketMQTemplate;
    private final LocalEventOutboxMapper outboxMapper;

    // 与现有 Canal item 同步共用一套 RabbitMQ 基础设施，新增 outbox 专用队列
    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = MQConstants.ORDER_CANAL_RABBITMQ_QUEUE, durable = "true"),
            exchange = @Exchange(name = MQConstants.ORDER_CANAL_RABBITMQ_EXCHANGE, type = ExchangeTypes.TOPIC),
            key = MQConstants.ORDER_CANAL_RABBITMQ_KEY
    ))
    @SuppressWarnings("unchecked")
    public void onOutboxInsert(Map<String, Object> canalMsg) {
        try {
            String type  = String.valueOf(canalMsg.get("type"));
            String table = String.valueOf(canalMsg.get("table"));

            // 只处理 local_event_outbox 表的 INSERT 事件
            if (!"INSERT".equals(type) || !"local_event_outbox".equals(table)) {
                return;
            }

            List<Map<String, Object>> dataList =
                    (List<Map<String, Object>>) canalMsg.get("data");
            if (dataList == null || dataList.isEmpty()) return;

            for (Map<String, Object> row : dataList) {
                processOutboxRow(row);
            }

        } catch (Exception e) {
            log.error("[Canal-Outbox] 处理 Canal 事件异常，消息将被 NACK 重试", e);
            throw new RuntimeException("Canal outbox 处理失败", e);
        }
    }

    private void processOutboxRow(Map<String, Object> row) {
        String eventType = String.valueOf(row.get("event_type"));
        String statusStr = String.valueOf(row.get("status"));
        Long   outboxId  = Long.valueOf(String.valueOf(row.get("id")));
        Long   orderId   = Long.valueOf(String.valueOf(row.get("order_id")));

        // 只处理 DB_ORDER_BROADCAST 且 status=0 的记录
        if (!"DB_ORDER_BROADCAST".equals(eventType) || !"0".equals(statusStr)) {
            return;
        }

        // 再次查库确认 status（防止 afterCommit 已成功，Canal 事件延迟到达的重复处理）
        // 注意：Canal 事件是异步的，afterCommit 可能已经将 status 改为 1
        var outbox = outboxMapper.selectById(outboxId);
        if (outbox == null || outbox.getStatus() == 1) {
            log.info("[Canal-Outbox] outbox 已被 afterCommit 处理，跳过。outboxId={}", outboxId);
            return;
        }

        String payload = String.valueOf(row.get("payload"));

        try {
            rocketMQTemplate.syncSend(
                    "TRADE_DB_ORDER_TOPIC",
                    MessageBuilder.withPayload(payload)
                            .setHeader(RocketMQHeaders.KEYS, String.valueOf(orderId))
                            .build()
            );
            // 标记已发送
            outbox.setStatus(1);
            outbox.setUpdateTime(LocalDateTime.now());
            outboxMapper.updateById(outbox);
            log.info("[Canal-Outbox] 补偿广播成功。orderId={}，outboxId={}", orderId, outboxId);

        } catch (Exception e) {
            log.error("[Canal-Outbox] 补偿广播失败，等待下次 Canal 重推。orderId={}", orderId, e);
            // 不更新 status，Canal 不会重推（INSERT 事件只来一次）
            // 此时需要 XXL-Job 作为最终兜底，或允许少量数据靠对账修复
            // 建议：保留 XXL-Job 扫描作为第三层兜底，间隔可以拉长到 5 分钟
        }
    }
}