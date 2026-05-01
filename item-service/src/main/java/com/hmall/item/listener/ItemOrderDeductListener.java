package com.hmall.item.listener;

import com.hmall.api.dto.OrderDetailDTO;
import com.hmall.common.exception.BizIllegalException;
import com.hmall.common.utils.RabbitMqHelper;
import com.hmall.item.constants.MQConstants;
import com.hmall.item.service.IItemService;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class ItemOrderDeductListener {

    private final IItemService itemService;
    private final RabbitMqHelper rabbitMqHelper;

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = MQConstants.ORDER_ITEM_DEDUCT_QUEUE, durable = "true"),
            exchange = @Exchange(name = MQConstants.ORDER_EVENT_EXCHANGE, type = ExchangeTypes.TOPIC),
            key = MQConstants.ORDER_ITEM_DEDUCT_KEY
    ))
    public void listenOrderCreated(Map<String, Object> msg,
                                   Channel channel,
                                   @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        Long orderId = msg.get("orderId") == null ? null : Long.valueOf(String.valueOf(msg.get("orderId")));
        try {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> detailsMap = (List<Map<String, Object>>) msg.get("details");
            List<OrderDetailDTO> details = detailsMap.stream()
                    .map(detail -> {
                        OrderDetailDTO dto = new OrderDetailDTO();
                        dto.setItemId(detail.get("itemId") == null ? null : Long.valueOf(String.valueOf(detail.get("itemId"))));
                        dto.setNum(detail.get("num") == null ? null : Integer.valueOf(String.valueOf(detail.get("num"))));
                        return dto;
                    })
                    .collect(java.util.stream.Collectors.toList());

            Long epoch = msg.get("epoch") == null ? null : Long.valueOf(String.valueOf(msg.get("epoch")));
            Long seq = msg.get("seq") == null ? null : Long.valueOf(String.valueOf(msg.get("seq")));
            String version = msg.get("version") == null ? null : String.valueOf(msg.get("version"));

            // 1. 执行数据库扣减，并沿用 Redis 预扣减链路生成的 epoch/seq/version 记录流水
            itemService.deductStock(orderId, details, epoch, seq, version);

            // 2. 扣减成功，手动 ACK 确认消费完成
            channel.basicAck(deliveryTag, false);
            log.info("订单 {} MySQL库存异步扣减成功，手动ACK完毕", orderId);

        } catch (BizIllegalException e) {
            log.error("【严重异常】订单 {} MySQL扣减库存不足触发超卖拦截！准备发起逆向回滚事务", orderId);

            // 应对策略：发 MQ 通知订单服务执行【逆向事务】
            rabbitMqHelper.sendMessageWithConfirm(
                    MQConstants.CANCEL_ORDER_EXCHANGE,
                    MQConstants.CANCEL_ORDER_KEY,
                    msg,
                    MQConstants.MAX_RETRY_TIMES
            );

            // 【重点】：既然我们已经发起了补偿，说明这个“库存不足”的错误已经被我们妥善处理了。
            // 因此这根消息不能留在队列里死循环，必须手动 ACK 删掉它！
            channel.basicAck(deliveryTag, false);

        } catch (Exception e) {
            log.error("订单 {} 扣减库存发生系统级异常（如数据库宕机），准备 NACK 触发重试", orderId, e);

            // 【重点】：系统异常，手动 NACK。
            // 第三个参数 requeue = true 表示让消息重新回到队列头部等待下次消费。
            channel.basicNack(deliveryTag, false, true);
        }
    }
}
