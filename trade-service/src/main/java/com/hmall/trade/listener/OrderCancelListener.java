package com.hmall.trade.listener;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hmall.api.dto.OrderDetailDTO;
import com.hmall.trade.constants.MQConstants;
import com.hmall.trade.service.IOrderService;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderCancelListener {

    private final IOrderService orderService;

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = MQConstants.CANCEL_ORDER_QUEUE, durable = "true"),
            exchange = @Exchange(name = MQConstants.CANCEL_ORDER_EXCHANGE, type = ExchangeTypes.TOPIC),
            key = MQConstants.CANCEL_ORDER_KEY
    ))
    public void listenCancelOrder(String payload, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        try {
            JSONObject json = JSON.parseObject(payload);
            Long orderId = json.getLong("orderId");
            List<OrderDetailDTO> details = json.getJSONArray("details").toJavaList(OrderDetailDTO.class);

            log.warn("收到订单 {} 的逆向取消消息，开始执行回滚逻辑...", orderId);

            // 执行回滚（关闭订单、退还 Redis 预扣库存）
            orderService.cancelOrderAndRestore(orderId, details);

            // 成功后手动 ACK
            channel.basicAck(deliveryTag, false);
            log.info("订单 {} 逆向回滚事务执行完毕，已手动 ACK", orderId);

        } catch (Exception e) {
            log.error("处理订单取消补偿消息失败，准备重试", e);
            // 异常时 NACK 重试
            channel.basicNack(deliveryTag, false, true);
        }
    }
}