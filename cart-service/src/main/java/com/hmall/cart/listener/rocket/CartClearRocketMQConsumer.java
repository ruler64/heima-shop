package com.hmall.cart.listener.rocket;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hmall.cart.constants.MQConstants;
import com.hmall.cart.service.ICartService;
import com.hmall.common.utils.UserContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 订单落库广播消费者（cart-service）。
 * 负责清空已下单商品的购物车。
 */
@Slf4j
@Service
@RocketMQMessageListener(
        topic = MQConstants.ROCKETMQ_DB_ORDER_TOPIC,
        consumerGroup = MQConstants.ROCKETMQ_CART_CLEAR_GROUP,
        consumeThreadNumber = 8,
        consumeTimeout = 15000L,
        maxReconsumeTimes = 16
)
@RequiredArgsConstructor
public class CartClearRocketMQConsumer implements RocketMQListener<String> {

    private final ICartService cartService;

    @Override
    public void onMessage(String msgJson) {
        JSONObject json = JSON.parseObject(msgJson);
        Long orderId = json.getLong("orderId");
        Long userId = json.getLong("userId");

        log.info("[购物车清理] 收到落库广播，开始清空购物车。orderId={}", orderId);

        try {
            // 解析 itemIds（兼容两种结构）
            List<Long> itemIds = parseItemIds(json);
            if (itemIds.isEmpty()) {
                log.warn("[购物车清理] itemIds 为空，跳过。orderId={}", orderId);
                return;
            }
            UserContext.setUser(userId);
            // removeByItemIds 内部应有幂等保护（删已删的数据不报错）
            cartService.removeByItemIds(itemIds);
            log.info("[购物车清理] 购物车清空成功。orderId={}", orderId);
        } finally {
            UserContext.removeUser();
        }
    }

    private List<Long> parseItemIds(JSONObject json) {
        Object detailsObj = json.get("details");
        if (detailsObj == null) {
            JSONObject orderForm = json.getJSONObject("orderForm");
            if (orderForm != null) detailsObj = orderForm.get("details");
        }
        if (!(detailsObj instanceof List)) return List.of();
        return ((List<?>) detailsObj).stream()
                .filter(d -> d instanceof java.util.Map)
                .map(d -> Long.valueOf(String.valueOf(((java.util.Map<?, ?>) d).get("itemId"))))
                .collect(Collectors.toList());
    }
}
