package com.hmall.trade.task;

import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.hmall.common.utils.RabbitMqHelper;
import com.hmall.trade.constants.MQConstants;
import com.hmall.trade.domain.po.LocalEventOutbox;
import com.hmall.trade.mapper.LocalEventOutboxMapper;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 专门负责扫描musql本地消息表 “通知Item服务恢复库存” 的兜底任务
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class ItemRestoreJobHandler {

    private final LocalEventOutboxMapper outboxMapper;
    private final RabbitMqHelper rabbitMqHelper;

    @XxlJob("publishRestoreItemEventsJob")
    public void publishRestoreItemEvents() {
        // 1. 分批捞出状态为 0 (待发送) 的任务
        List<LocalEventOutbox> tasks = outboxMapper.selectList(
                new LambdaQueryWrapper<LocalEventOutbox>()
                        .eq(LocalEventOutbox::getEventType, "RESTORE_ITEM_STOCK")
                        .eq(LocalEventOutbox::getStatus, 0)
                        .last("LIMIT 100")
        );

        if (tasks == null || tasks.isEmpty()) return;

        for (LocalEventOutbox event : tasks) {
            try {
                // 2. 发送 MQ 消息给 Item 服务，使用 confirm 机制确保不丢消息
                rabbitMqHelper.sendMessageWithConfirm(
                        MQConstants.RESTORE_ITEM_EXCHANGE,
                        MQConstants.RESTORE_ITEM_KEY,
                        event.getPayload(), // payload 里面有 orderId 和 details
                        MQConstants.MAX_RETRY_TIMES
                );

                // 3. 只要 MQ 确认收到了（没抛异常），说明任务投递成功，标记为已完成
                event.setStatus(1);
                event.setUpdateTime(LocalDateTime.now());
                outboxMapper.updateById(event);

                log.info("定时任务成功发送库存恢复 MQ 消息，订单号: {}", JSON.parseObject(event.getPayload()).getString("orderId"));

            } catch (Exception e) {
                // 【精髓所在】如果 RabbitMQ 连不上，或者发消息超时，这里会报错。
                // 但是！我们绝对不去修改 event 的 status。
                // 这样下一分钟 XXL-Job 再次执行时，它依然会被捞出来重发，绝对不会丢数据！
                log.error("发送库存恢复 MQ 消息失败，事件ID: {}，等待下次调度重试", event.getId(), e);
            }
        }
    }
}