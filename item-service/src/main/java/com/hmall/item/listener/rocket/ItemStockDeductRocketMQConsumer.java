package com.hmall.item.listener.rocket;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hmall.api.dto.OrderDetailDTO;
import com.hmall.item.constants.MQConstants;
import com.hmall.item.service.IItemService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 订单落库广播消费者（item-service）。
 * 负责扣减 MySQL 库存（已含幂等保护）。
 */
@Slf4j
@Service
@RocketMQMessageListener(
        topic = MQConstants.ROCKETMQ_DB_ORDER_TOPIC,
        consumerGroup = MQConstants.ROCKETMQ_ITEM_DEDUCT_GROUP,
        consumeThreadNumber = 8,
        consumeTimeout = 15000L,
        maxReconsumeTimes = 16
)
@RequiredArgsConstructor
public class ItemStockDeductRocketMQConsumer implements RocketMQListener<String> {

    private final IItemService itemService;

    @Override
    public void onMessage(String msgJson) {
        JSONObject json = JSON.parseObject(msgJson);
        Long orderId = json.getLong("orderId");
        log.info("[item扣库] 收到落库广播，开始扣减 MySQL 库存。orderId={}", orderId);

        // 解析 details
        List<OrderDetailDTO> details = parseDetails(json);

        // deductStock 内部已有幂等保护（StockDeductLog 唯一索引），重复消费安全
        itemService.deductStock(orderId, details);

        log.info("[item扣库] MySQL 库存扣减成功。orderId={}", orderId);
        // 不抛异常 = ACK；抛异常 = RocketMQ 阶梯重试
    }

    private List<OrderDetailDTO> parseDetails(JSONObject json) {
        // 兼容两种消息结构
        Object detailsObj = json.get("details");
        if (detailsObj == null) {
            JSONObject orderForm = json.getJSONObject("orderForm");
            if (orderForm != null) detailsObj = orderForm.get("details");
        }
        if (detailsObj == null) {
            throw new IllegalArgumentException("消息缺少 details，orderId=" + json.getLong("orderId"));
        }
        return JSON.parseArray(JSON.toJSONString(detailsObj), OrderDetailDTO.class);
    }
}
