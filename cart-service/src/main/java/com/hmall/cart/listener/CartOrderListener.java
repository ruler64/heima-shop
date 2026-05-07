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
import java.util.Objects;
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
        try {
            if (msg == null) {
                throw new IllegalArgumentException("订单消息为空");
            }

            Long userId = msg.get("userId") == null ? null : Long.valueOf(String.valueOf(msg.get("userId")));
            List<Long> itemIds = parseItemIds(msg);

            if (itemIds.isEmpty()) {
                throw new IllegalArgumentException("订单消息中的 itemIds 为空");
            }

            UserContext.setUser(userId);
            cartService.removeByItemIds(itemIds);
            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            channel.basicNack(deliveryTag, false, true);
            throw e;
        } finally {
            UserContext.removeUser();
        }
    }

    @SuppressWarnings("unchecked")
    private List<Long> parseItemIds(Map<String, Object> msg) {
        Object itemIdsObj = msg.get("itemIds");
        if (itemIdsObj instanceof List && !((List<?>) itemIdsObj).isEmpty()) {
            return ((List<?>) itemIdsObj).stream()
                    .filter(Objects::nonNull)
                    .map(itemId -> Long.valueOf(String.valueOf(itemId)))
                    .collect(Collectors.toList());
        }

        Object detailsObj = msg.get("details");
        if (detailsObj == null) {
            Object orderFormObj = msg.get("orderForm");
            if (orderFormObj instanceof Map) {
                detailsObj = ((Map<String, Object>) orderFormObj).get("details");
            }
        }
        if (!(detailsObj instanceof List) || ((List<?>) detailsObj).isEmpty()) {
            throw new IllegalArgumentException("订单消息缺少 itemIds/details，msgKeys=" + msg.keySet());
        }

        return ((List<?>) detailsObj).stream()
                .filter(Objects::nonNull)
                .map(detail -> {
                    if (detail instanceof Map) {
                        Object itemId = ((Map<String, Object>) detail).get("itemId");
                        if (itemId == null) {
                            throw new IllegalArgumentException("订单明细缺少 itemId，detail=" + detail);
                        }
                        return Long.valueOf(String.valueOf(itemId));
                    }
                    throw new IllegalArgumentException("订单明细格式非法，detail=" + detail);
                })
                .collect(Collectors.toList());
    }
}
