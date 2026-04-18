package com.hmall.cart.listener;

import com.hmall.cart.constants.MQConstants;
import com.hmall.cart.service.ICartService;
import com.hmall.common.utils.UserContext;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
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
import java.util.stream.Collectors;


@Component
@RequiredArgsConstructor
public class CartOrderListener {

    private final ICartService cartService;

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = MQConstants.ORDER_CART_CLEAR_QUEUE, durable = "true"),
            exchange = @Exchange(name = MQConstants.ORDER_EVENT_EXCHANGE, type = ExchangeTypes.TOPIC),
            key = MQConstants.ORDER_CART_CLEAR_KEY
    ))
    public void listenOrderCreated(Map<String, Object> msg,
                                   Channel channel,
                                   @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        Long userId = msg.get("userId") == null ? null : Long.valueOf(String.valueOf(msg.get("userId")));
        @SuppressWarnings("unchecked")
        List<Object> rawItemIds = (List<Object>) msg.get("itemIds");
        List<Long> itemIds = rawItemIds.stream()
                .map(itemId -> Long.valueOf(String.valueOf(itemId)))
                .collect(Collectors.toList());

        UserContext.setUser(userId);
        try {
            cartService.removeByItemIds(itemIds);
            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            channel.basicNack(deliveryTag, false, true);
            throw e;
        } finally {
            UserContext.removeUser();
        }
    }
}
