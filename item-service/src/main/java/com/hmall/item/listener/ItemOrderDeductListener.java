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
import java.util.Objects;

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
        Long orderId = msg == null || msg.get("orderId") == null ? null : Long.valueOf(String.valueOf(msg.get("orderId")));
        try {
            if (msg == null) {
                throw new IllegalArgumentException("订单消息为空");
            }
            List<OrderDetailDTO> details = parseDetails(msg);

            // 1. 执行数据库扣减。MySQL 版本由 item 服务独立维护，不接收 Redis epoch/seq。
            itemService.deductStock(orderId, details);

            // 2. 扣减成功，手动 ACK 确认消费完成
            channel.basicAck(deliveryTag, false);
            log.info("订单 {} MySQL库存异步扣减成功，手动ACK完毕", orderId);

        } catch (BizIllegalException e) {
            log.error("【严重异常】订单 {} MySQL扣减库存不足触发超卖拦截！准备发起逆向回滚事务", orderId, e);

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

    @SuppressWarnings("unchecked")
    private List<OrderDetailDTO> parseDetails(Map<String, Object> msg) {
        Object detailsObj = msg.get("details");
        if (detailsObj == null) {
            Object orderFormObj = msg.get("orderForm");
            if (orderFormObj instanceof Map) {
                detailsObj = ((Map<String, Object>) orderFormObj).get("details");
            }
        }
        if (!(detailsObj instanceof List) || ((List<?>) detailsObj).isEmpty()) {
            throw new IllegalArgumentException("订单消息缺少商品明细，msgKeys=" + msg.keySet());
        }

        List<?> rawDetails = (List<?>) detailsObj;
        List<OrderDetailDTO> details = rawDetails.stream()
                .filter(Objects::nonNull)
                .map(detail -> {
                    if (detail instanceof OrderDetailDTO) {
                        return (OrderDetailDTO) detail;
                    }
                    if (!(detail instanceof Map)) {
                        throw new IllegalArgumentException("订单明细格式非法，detail=" + detail);
                    }
                    Map<String, Object> detailMap = (Map<String, Object>) detail;
                    Object itemId = detailMap.get("itemId");
                    Object num = detailMap.get("num");
                    if (itemId == null || num == null) {
                        throw new IllegalArgumentException("订单明细缺少 itemId 或 num，detail=" + detailMap);
                    }
                    OrderDetailDTO dto = new OrderDetailDTO();
                    dto.setItemId(Long.valueOf(String.valueOf(itemId)));
                    dto.setNum(Integer.valueOf(String.valueOf(num)));
                    return dto;
                })
                .collect(java.util.stream.Collectors.toList());
        if (details.isEmpty()) {
            throw new IllegalArgumentException("订单消息中的商品明细为空，msgKeys=" + msg.keySet());
        }
        return details;
    }
}
