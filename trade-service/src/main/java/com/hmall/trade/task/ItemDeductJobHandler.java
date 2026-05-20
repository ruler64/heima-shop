package com.hmall.trade.task;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.hmall.common.utils.CollUtils;
import com.hmall.trade.constants.MQConstants;
import com.hmall.trade.domain.po.LocalEventOutbox;
import com.hmall.trade.mapper.LocalEventOutboxMapper;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 兜底补偿：扫描 MySQL outbox，补发下游广播。
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class ItemDeductJobHandler {

    private final LocalEventOutboxMapper outboxMapper;
    private final RocketMQTemplate rocketMQTemplate;

    @XxlJob("publishOrderEventsJob")
    public void publishOrderEvents() {
        // 1. 分批捞出状态为 0 (待发送) 的记录
        List<LocalEventOutbox> tasks = outboxMapper.selectList(
                new LambdaQueryWrapper<LocalEventOutbox>()
                        .eq(LocalEventOutbox::getStatus, 0)
                        .eq(LocalEventOutbox::getEventType, "DB_ORDER_BROADCAST")
                        .lt(LocalEventOutbox::getCreateTime, LocalDateTime.now().minusSeconds(30))
                        .last("LIMIT 100")
        );

        if (CollUtils.isEmpty(tasks)) {
            return;
        }

        for (LocalEventOutbox event : tasks) {
            try {
                rocketMQTemplate.syncSend(
                        MQConstants.ROCKETMQ_DB_ORDER_TOPIC,
                        MessageBuilder.withPayload(event.getPayload())
                                .setHeader(org.apache.rocketmq.spring.support.RocketMQHeaders.KEYS,
                                        String.valueOf(event.getOrderId()))
                                .build()
                );
                log.info("订单 {} 事务提交成功，已发出消息给下游：15分钟延迟关单检测、item扣减库存、cart清空购物车", event.getOrderId());
                // 3. 投递成功，更新状态为 1 (已发送)
                event.setStatus(1);
                event.setUpdateTime(LocalDateTime.now());
                outboxMapper.updateById(event);
                log.info("XXL-JOB 成功补偿投递订单广播，事件ID={}, orderId={}", event.getId(), event.getOrderId());
            } catch (Exception e) {
                log.error("XXL-JOB 补偿投递订单广播失败，待下次重试。事件ID={}", event.getId(), e);
            }
        }
    }
}
