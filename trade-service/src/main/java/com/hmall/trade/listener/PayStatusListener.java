package com.hmall.trade.listener;

import com.hmall.api.client.PayClient;
import com.hmall.trade.domain.po.Order;
import com.hmall.trade.service.IOrderService;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.*;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class PayStatusListener {

    private final IOrderService orderService;

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = "trade.pay.success.queue",durable = "true"),
            exchange = @Exchange(name = "pay.direct",type = ExchangeTypes.DIRECT),
            arguments = @Argument(name = "x-queue-mode", value = "lazy"), //声明惰性队列，持久化存储，更好的性能
            key = {"pay.success"}
    ))
    public void listenPaySuccess(Long orderId){
        // 1.查询订单
        Order order = orderService.getById(orderId);
        // 2.判断订单状态，是否为未支付
        if (order == null || order.getStatus() != 1){
            // 不做处理
            return;
        }
        // 3.标记订单状态为已支付
        orderService.markOrderPaySuccess(orderId);
    }
}
