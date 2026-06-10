package com.hmall.trade.task;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.hmall.trade.constants.MQConstants;
import com.hmall.trade.domain.po.LocalEventOutbox;
import com.hmall.trade.mapper.LocalEventOutboxMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import com.xxl.job.core.handler.annotation.XxlJob;

import java.time.LocalDateTime;
import java.util.List;

/**
 * xxlJob中暂未实现compensateFailedOutboxTasks配置
 * 防止在插入outbox信息后，由于服务和canal同时宕机导致的补偿消息没发送出去，由xxlJob扫表来实现
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class CompensateFailedOutboxJob {

    private final LocalEventOutboxMapper outboxMapper;
    private final RocketMQTemplate rocketMQTemplate;

    @XxlJob("compensateFailedOutboxTasks")
    public void compensate() {
        // 1. 捞取 3 分钟前创建且依然为 0 的垃圾数据
        List<LocalEventOutbox> sysExceptions = outboxMapper.selectList(
            new LambdaQueryWrapper<LocalEventOutbox>()
                .eq(LocalEventOutbox::getStatus, 0)
                .le(LocalEventOutbox::getCreateTime, LocalDateTime.now().minusMinutes(3))
                .last("LIMIT 200") // 批处理防爆
        );

        for (LocalEventOutbox outbox : sysExceptions) {
            try {
                // 判断 topic 类型发送
                String topic = MQConstants.OUTBOX_EVENT_CANCEL_RESTORE.equals(outbox.getEventType())
                        ? MQConstants.ROCKETMQ_CANCEL_TOPIC : MQConstants.ROCKETMQ_DB_ORDER_TOPIC;
                
                rocketMQTemplate.syncSend(topic, MessageBuilder.withPayload(outbox.getPayload()).build());
                
                // 更新为成功
                outboxMapper.updateStatus(outbox.getId(), 1);
                log.info("[Outbox 终极补偿] 成功修复死单消息. outboxId={}", outbox.getId());
            } catch (Exception e) {
                log.error("[Outbox 终极补偿] 消息发送依然失败，等待下次调度. outboxId={}", outbox.getId(), e);
            }
        }
    }
}