package com.hmall.trade.listener;

import com.hmall.api.client.PayClient;
import com.hmall.api.dto.OrderDetailDTO;
import com.hmall.api.dto.PayOrderDTO;
import com.hmall.common.utils.BeanUtils;
import com.hmall.trade.constants.MQConstants;
import com.hmall.trade.domain.enmu.OrderStatusEnum;
import com.hmall.trade.domain.po.Order;
import com.hmall.trade.domain.po.OrderDetail;
import com.hmall.trade.service.IOrderDetailService;
import com.hmall.trade.service.IOrderService;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.*;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderDelayMessageListener {
    private final IOrderService orderService;
    private final IOrderDetailService orderDetailService; // 新增：注入订单明细服务
    private final PayClient payClient;

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = MQConstants.DELAY_ORDER_QUEUE_NAME),
            exchange = @Exchange(name = MQConstants.DELAY_EXCHANGE_NAME,delayed = "true"),//开启delayed改造为延迟消息交换机
            //arguments = @Argument(name = "x-queue-mode", value = "lazy"), //声明惰性队列，持久化存储，更好的性能
            key = MQConstants.DELAY_ORDER_KEY
    ))
    public void listenOrderDelayMessage(Long orderId, Channel channel, Message message) throws Exception {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();

        try {
            // 1. 查询订单
            Order order = orderService.getById(orderId);

            // 2. 检测订单状态，判断是否已支付 (1是未支付)
            if (order == null || OrderStatusEnum.getByCode(order.getStatus()) != OrderStatusEnum.UNPAID) {
                // 【大厂细节 1】订单不存在，或者已经被支付/取消了，说明业务已经处理完毕。
                // 必须直接 ACK 丢弃消息，千万不能抛异常或不处理，否则会无限死循环！
                log.info("延迟关单检测：订单 {} 状态非未支付状态，无需关单，直接放行", orderId);
                channel.basicAck(deliveryTag, false);
                return;
            }

            // 3. 未支付，需要查询支付流水状态 (防并发兜底，可能微信支付刚成功但由于网络延迟回调没到)
            PayOrderDTO payOrder = payClient.queryPayOrderByBizOrderNo(orderId);

            // 4. 判断实际是否支付
            if (payOrder != null && OrderStatusEnum.getByCode(payOrder.getStatus()) == OrderStatusEnum.DELIVERED) {
                // 4.1 支付中心显示已支付，但我们本地订单没更新，帮忙更新一下
                orderService.markOrderPaySuccess(orderId);
                log.info("延迟关单检测：订单 {} 实际已支付，主动更新订单状态成功", orderId);
            } else {
                // 4.2 确认未支付
                // 第一步：根据 orderId 查询出该订单下的所有商品明细
                List<OrderDetail> detailList = orderDetailService.lambdaQuery()
                        .eq(OrderDetail::getOrderId, orderId)
                        .list();

                // 第二步：将 PO 转换为 DTO (根据你项目实际的 Bean 拷贝工具)
                List<OrderDetailDTO> details = BeanUtils.copyList(detailList, OrderDetailDTO.class);

                // 第三步：调用全新的、基于 本地消息表 的 异步 关单退库方法！
                orderService.cancelOrderAndRestore(orderId, details);

                log.info("延迟关单检测：订单 {} 超时未支付，执行新版异步关单并记录退库任务成功", orderId);
            }

            // 5. 业务全部执行成功，执行手动 ACK
            channel.basicAck(deliveryTag, false);

        } catch (Exception e) {
            log.error("处理延迟关单消息发生异常，准备重试。订单号: {}", orderId, e);
            // 【大厂细节 2】发生真实业务异常（如数据库宕机、Feign调用超时），执行 NACK 重新入队
            channel.basicNack(deliveryTag, false, true);
        }
    }
}
