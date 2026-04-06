package com.hmall.trade.listener;

import com.hmall.trade.domain.enmu.OrderStatusEnum;
import com.hmall.trade.domain.po.Order;
import com.hmall.trade.service.IOrderService;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.*;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PayStatusListener {

    private final IOrderService orderService;

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = "trade.pay.success.queue", durable = "true"),
            exchange = @Exchange(name = "pay.direct", type = ExchangeTypes.DIRECT),
            arguments = @Argument(name = "x-queue-mode", value = "lazy"),
            key = {"pay.success"}
    ))
    public void listenPaySuccess(Long orderId, Channel channel, Message message) throws Exception {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();

        try {
            // 1. 查询订单
            Order order = orderService.getById(orderId);

            // 2. 判断订单状态，是否为未支付 (1是未支付)
            if (order == null || OrderStatusEnum.getByCode(order.getStatus()) != OrderStatusEnum.UNPAID) {
                // 【大厂细节 3】幂等性过滤。如果状态已经是已支付了(比如重复回调)，直接 ACK 丢弃
                log.info("接收到支付成功消息，但订单 {} 状态已被修改(重复回调)，直接放行", orderId);
                channel.basicAck(deliveryTag, false);
                return;
            }

            // 3. 标记订单状态为已支付 (此方法基于 UPDATE 乐观锁机制)
            orderService.markOrderPaySuccess(orderId);
            log.info("接收到支付成功消息，订单 {} 状态更新为已支付成功", orderId);

            // 4. 业务执行完毕，手动 ACK
            channel.basicAck(deliveryTag, false);

        } catch (Exception e) {
            log.error("处理支付成功消息发生异常，准备重试。订单号: {}", orderId, e);
            // 异常，重回队列
            channel.basicNack(deliveryTag, false, true);
        }
    }
}