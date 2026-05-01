package com.hmall.trade.task;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.hmall.common.utils.CollUtils;
import com.hmall.common.utils.RabbitMqHelper;
import com.hmall.trade.constants.MQConstants;
import com.hmall.trade.domain.po.LocalEventOutbox;
import com.hmall.trade.mapper.LocalEventOutboxMapper;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * 利用MySQL中的本地消息表，发送消息给item服务，让他扣减库存，补偿投递延时消息，并且更新MySQL中的本地消息表中该orderId为出已处理的状态
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class ItemDeductJobHandler {

    private final LocalEventOutboxMapper outboxMapper;
    private final RabbitTemplate rabbitTemplate;
    private final RabbitMqHelper rabbitMqHelper;

    @XxlJob("publishOrderEventsJob")
    public void publishOrderEvents() {
        // 1. 分批捞出状态为 0 (待发送) 的记录
        List<LocalEventOutbox> tasks = outboxMapper.selectList(
                new LambdaQueryWrapper<LocalEventOutbox>()
                        .eq(LocalEventOutbox::getStatus, 0)
                        .eq(LocalEventOutbox::getEventType, "ORDER_CREATED")
                        .last("LIMIT 100") // 每次处理100条，防止内存溢出
        );

        if (CollUtils.isEmpty(tasks)) return;

        for (LocalEventOutbox event : tasks) {
            try {
                // 2. 重新投递到 MQ
                // outbox 里存的是 JSON 字符串，这里必须反序列化成对象再发 MQ，
                // 否则 RabbitTemplate 会把它当“普通 String”再包一层，消费者拿到的就是双重编码字符串。
                Map<String, Object> payload = com.alibaba.fastjson.JSON.parseObject(event.getPayload(), java.util.Map.class);
                long orderId = (Long)payload.get("orderId");
                Object version = payload.get("version");
                Object epoch = payload.get("epoch");
                rabbitMqHelper.sendMessageWithConfirm(
                        MQConstants.ORDER_EVENT_EXCHANGE,
                        MQConstants.ORDER_ITEM_DEDUCT_KEY,
                        payload,
                        MQConstants.MAX_RETRY_TIMES
                );

                rabbitMqHelper.sendMessageWithConfirm(
                        MQConstants.ORDER_CART_CLEAR_EXCHANGE,
                        MQConstants.ORDER_CART_CLEAR_KEY,
                        payload,
                        MQConstants.MAX_RETRY_TIMES
                );
                rabbitTemplate.convertAndSend(
                        MQConstants.DELAY_EXCHANGE_NAME,
                        MQConstants.DELAY_ORDER_KEY,
                        orderId,
                        message -> {
                            message.getMessageProperties().setDelay(15 * 60 * 1000); // 15分钟
                            return message;
                        }
                );
                log.info("订单 {} 事务提交成功，已发出15分钟延迟关单检测消息，version={}, epoch={}", orderId, version, epoch);
                // 3. 投递成功，更新状态为 1 (已发送)
                event.setStatus(1);
                event.setUpdateTime(LocalDateTime.now());
                outboxMapper.updateById(event);

                log.info("XXL-JOB 成功补偿投递订单事件，事件ID: {}", event.getId());
            } catch (Exception e) {
                // 失败不更新状态，等下一次任务重试
                log.error("XXL-JOB 补偿投递事件失败，待下次重试。事件ID: {}", event.getId(), e);
            }
        }
    }
}

