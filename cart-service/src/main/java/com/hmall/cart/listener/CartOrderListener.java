package com.hmall.cart.listener;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hmall.cart.constants.MQConstants;
import com.hmall.cart.service.ICartService;
import com.hmall.common.utils.UserContext;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class CartOrderListener {

    private final ICartService cartService;

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = MQConstants.ORDER_CART_CLEAR_QUEUE, durable = "true"),
            exchange = @Exchange(name = MQConstants.ORDER_EVENT_EXCHANGE, type = ExchangeTypes.TOPIC),
            key = MQConstants.ORDER_CART_CLEAR_KEY
    ))
    public void listenOrderCreated(String payload) {
        JSONObject json = JSON.parseObject(payload);
        Long userId = json.getLong("userId");
        List<Long> itemIds = json.getJSONArray("itemIds").toJavaList(Long.class);

        // 购物车清理本身是天然幂等的（删除了就没有了），直接调用
        UserContext.setUser(userId); // 模拟用户上下文
        try {
            cartService.removeByItemIds(itemIds);
        } finally {
            UserContext.removeUser();
        }
    }
}
