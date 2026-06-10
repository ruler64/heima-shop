package com.hmall.trade.listener.canal;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hmall.trade.constants.MQConstants;
import com.hmall.trade.domain.po.LocalEventOutbox;
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
 * Canal 监听 local_event_outbox 表 INSERT 事件，补偿 afterCommit 发送失败的消息。
 *
 * 支持的 event_type：
 *   DB_ORDER_BROADCAST   → ROCKETMQ_DB_ORDER_TOPIC   （下单广播，原有）
 *   CANCEL_RESTORE_STOCK → ROCKETMQ_CANCEL_TOPIC      （取消恢复库存，新增）
 *
 * 补偿流程（两种 event_type 完全对称）：
 *   Canal 感知 outbox INSERT → 查库确认 status=0 → syncSend RocketMQ → updateStatus(1)
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

    /**
     * 核心分发：按 event_type 路由到对应 RocketMQ topic
     * @param row 对应数据库中的每行插入数据
     */
    private void processOutboxRow(Map<String, Object> row) {
        String eventType = String.valueOf(row.get("event_type"));
        String statusStr = String.valueOf(row.get("status"));
        Long   outboxId  = Long.valueOf(String.valueOf(row.get("id")));
        Long   orderId   = Long.valueOf(String.valueOf(row.get("order_id")));

        // 只处理 status=0 的待补偿记录
        if (!"0".equals(statusStr)) {
            return;
        }

        // 再次查库确认 status（防止 afterCommit 已成功，Canal 事件延迟到达的重复处理）
        // 注意：Canal 事件是异步的，afterCommit 可能已经将 status 改为 1
        LocalEventOutbox outbox = outboxMapper.selectById(outboxId);
        if (outbox == null || outbox.getStatus() == 1) {
            log.info("[Canal-Outbox] outbox 已被 afterCommit 处理，跳过。outboxId={}", outboxId);
            return;
        }

        String payload = String.valueOf(row.get("payload"));

        // 按 event_type 分发到不同 RocketMQ topic
        switch (eventType) {
            case MQConstants.OUTBOX_EVENT_ORDER_BROADCAST :
                    doSendAndMarkDone(MQConstants.ROCKETMQ_DB_ORDER_TOPIC,
                            orderId, outboxId, payload, "下单广播");
                    break;
            case MQConstants.OUTBOX_EVENT_CANCEL_RESTORE :
                    doSendAndMarkDone(MQConstants.ROCKETMQ_CANCEL_TOPIC,
                            orderId, outboxId, payload, "取消恢复库存");
                    break;
            default :
                    log.warn("[Canal-Outbox] 未知 event_type={}，跳过。outboxId={}", eventType, outboxId);
                    break;
        }
    }

    /**
     * 公共：发送 RocketMQ 消息并标记 outbox 完成
     * 两种 event_type 逻辑完全对称，提取避免重复
     * @param topic 话题
     * @param orderId 订单id
     * @param outboxId 消息表id
     * @param payload 负载
     * @param scene 事件名称
     */
    private void doSendAndMarkDone(String topic, Long orderId, Long outboxId,
                                   String payload, String scene) {
        try {
            rocketMQTemplate.syncSend(
                    topic,
                    MessageBuilder.withPayload(payload)
                            .setHeader(RocketMQHeaders.KEYS, String.valueOf(orderId))
                            .build()
            );
            // 标记已发送，防止下次 XXL-Job 扫描时重复处理
            LocalEventOutbox done = new LocalEventOutbox();
            done.setId(outboxId);
            done.setStatus(1);
            done.setUpdateTime(LocalDateTime.now());
            outboxMapper.updateById(done);
            log.info("[Canal-Outbox] {} 补偿成功。orderId={}，outboxId={}", scene, orderId, outboxId);

        } catch (Exception e) {
            // 发送失败：不更新 status，由 XXL-Job 兜底扫描
            log.error("[Canal-Outbox] {} 补偿发送失败，等待 XXL-Job 兜底。orderId={}", scene, orderId, e);
        }
    }
}