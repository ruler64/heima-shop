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
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderDelayMessageListener {
    //private static final int PAY_STATUS_PENDING_SUBMIT = 0;
    private static final int PAY_STATUS_WAITING = 1;//未付款
    //private static final int PAY_STATUS_TIMEOUT_OR_CANCELLED = 2;//明确超时/取消
    private static final int PAY_STATUS_SUCCESS = 2;//订单的状态，1、未付款 2、已付款,未发货 3、已发货,未确认 4、确认收货，交易成功 5、交易取消，订单关闭 6、交易结束，已评价
    private static final int MAX_RECHECK_TIMES = 3;//最大重试次数
    private static final int RECHECK_DELAY_MILLIS = 10 * 1000;

    private final IOrderService orderService;
    private final IOrderDetailService orderDetailService;
    private final PayClient payClient;
    private final RabbitTemplate rabbitTemplate;

    @RabbitListener(
            concurrency = "16-64",
            bindings = @QueueBinding(
            value = @Queue(name = MQConstants.DELAY_ORDER_QUEUE_NAME),
            exchange = @Exchange(name = MQConstants.DELAY_EXCHANGE_NAME, delayed = "true"),
            //arguments = @Argument(name = "x-queue-mode", value = "lazy"), //声明惰性队列，持久化存储，更好的性能
            key = MQConstants.DELAY_ORDER_KEY
    ))
    public void listenOrderDelayMessage(Long orderId, Channel channel, Message message) throws Exception {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        int recheckTimes = getRecheckTimes(message);

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

            // 3. 未支付，需要查询支付流水状态。支付状态未知/进行中时不立刻取消，先短延迟复查，规避 T14:59 支付与 T15:00 关单竞态。
            PayOrderDTO payOrder = payClient.queryPayOrderByBizOrderNo(orderId);
            Integer payStatus = payOrder == null ? null : payOrder.getStatus();

            if (Integer.valueOf(PAY_STATUS_SUCCESS).equals(payStatus)) {
                // 3.1 支付中心显示已支付，但我们本地订单没更新，帮忙更新一下
                orderService.markOrderPaySuccess(orderId);
                log.info("延迟关单检测：订单 {} 实际已支付，主动更新订单状态成功", orderId);
                channel.basicAck(deliveryTag, false);
                return;
            }

            if (needRecheck(payStatus, recheckTimes)) {
                sendRecheckMessage(orderId, recheckTimes + 1);
                log.warn("延迟关单检测：订单 {} 支付状态未知或仍在处理中，{} 秒后第 {} 次复查，payStatus={}",
                        orderId, RECHECK_DELAY_MILLIS / 1000, recheckTimes + 1, payStatus);
                channel.basicAck(deliveryTag, false);
                return;
            }

            // 4. (冗余逻辑)只有支付中心明确超时/取消，或者多次复查后仍非成功，才进入关单。
//            if (payStatus != null && !Integer.valueOf(PAY_STATUS_TIMEOUT_OR_CANCELLED).equals(payStatus) && recheckTimes < MAX_RECHECK_TIMES) {
//                sendRecheckMessage(orderId, recheckTimes + 1);
//                log.warn("延迟关单检测：订单 {} 支付状态未到终态，延迟复查，payStatus={}, recheckTimes={}", orderId, payStatus, recheckTimes + 1);
//                channel.basicAck(deliveryTag, false);
//                return;
//            }

            // 5. 确认未支付或复查耗尽，执行关单与退库任务。
            List<OrderDetail> detailList = orderDetailService.lambdaQuery()
                    .eq(OrderDetail::getOrderId, orderId)
                    .list();
            List<OrderDetailDTO> details = BeanUtils.copyList(detailList, OrderDetailDTO.class);
            orderService.cancelOrderAndRestore(orderId, details);
            log.info("延迟关单检测：订单 {} 超时未支付，执行新版异步关单并记录退库任务成功，payStatus={}, recheckTimes={}",
                    orderId, payStatus, recheckTimes);

            // 6. 业务全部执行成功，执行手动 ACK
            channel.basicAck(deliveryTag, false);

        } catch (Exception e) {
            log.error("处理延迟关单消息发生异常，准备重试。订单号: {}", orderId, e);
            // 【大厂细节 2】发生真实业务异常（如数据库宕机、Feign调用超时），执行 NACK 重新入队
            channel.basicNack(deliveryTag, false, true);
        }
    }

    private boolean needRecheck(Integer payStatus, int recheckTimes) {
        if (recheckTimes >= MAX_RECHECK_TIMES) {
            return false;
        }
        return payStatus == null
                || Integer.valueOf(PAY_STATUS_WAITING).equals(payStatus);
    }

    private int getRecheckTimes(Message message) {
        Object value = message.getMessageProperties().getHeaders().get("x-order-pay-recheck-times");
        if (value == null) {
            return 0;
        }
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (Exception e) {
            return 0;
        }
    }

    private void sendRecheckMessage(Long orderId, int recheckTimes) {
        rabbitTemplate.convertAndSend(
                MQConstants.DELAY_EXCHANGE_NAME,
                MQConstants.DELAY_ORDER_KEY,
                orderId,
                msg -> {
                    msg.getMessageProperties().setDelay(RECHECK_DELAY_MILLIS);
                    msg.getMessageProperties().setHeader("x-order-pay-recheck-times", recheckTimes);
                    return msg;
                }
        );
    }
}
