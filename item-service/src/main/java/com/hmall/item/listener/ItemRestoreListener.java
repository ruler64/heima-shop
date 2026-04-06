package com.hmall.item.listener;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hmall.api.dto.OrderDetailDTO;
import com.hmall.item.constants.MQConstants;
import com.hmall.item.service.IItemService;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class ItemRestoreListener {

    private final IItemService itemService;

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = MQConstants.RESTORE_ITEM_QUEUE, durable = "true"),
            exchange = @Exchange(name = MQConstants.RESTORE_ITEM_EXCHANGE, type = ExchangeTypes.TOPIC),
            key = MQConstants.RESTORE_ITEM_KEY
    ))
    public void listenRestoreItemStock(String payload, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        JSONObject json = JSON.parseObject(payload);
        Long orderId = json.getLong("orderId");

        try {
            List<OrderDetailDTO> details = json.getJSONArray("details").toJavaList(OrderDetailDTO.class);

            log.info("商品服务收到订单 {} 的逆向退库消息，开始恢复数据库库存...", orderId);

            // 调用你现有的、带有强幂等保护的增加库存方法
            itemService.increaseStock(orderId, details);

            // 成功后，手动 ACK，将这根消息彻底从 MQ 中删掉
            channel.basicAck(deliveryTag, false);
            log.info("订单 {} 数据库库存恢复完毕，手动 ACK 成功", orderId);

        } catch (DuplicateKeyException e) {
            // 【幂等防御】如果 increaseStock 内部利用数据库流水表防重放抛出了异常
            // 说明这个订单的库存以前已经退过了，直接 ACK 放行！
            log.warn("订单 {} 库存重复恢复，触发幂等拦截，直接 ACK 放行", orderId);
            channel.basicAck(deliveryTag, false);

        } catch (Exception e) {
            // 系统异常（数据库宕机等），NACK 让 MQ 重新入队，无限重试直到成功
            log.error("订单 {} 恢复库存发生异常，准备 NACK 重试", orderId, e);
            channel.basicNack(deliveryTag, false, true);
        }
    }
}