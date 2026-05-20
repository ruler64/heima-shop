package com.hmall.trade.listener.rocket;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hmall.api.client.PayClient;
import com.hmall.api.dto.OrderDetailDTO;
import com.hmall.api.dto.PayOrderDTO;
import com.hmall.common.utils.BeanUtils;
import com.hmall.trade.constants.MQConstants;
import com.hmall.trade.constants.RedisConstants;
import com.hmall.trade.domain.enmu.OrderStatusEnum;
import com.hmall.trade.domain.po.Order;
import com.hmall.trade.domain.po.OrderDetail;
import com.hmall.trade.service.IOrderDetailService;
import com.hmall.trade.service.IOrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RocketMQMessageListener(
        topic = MQConstants.ROCKETMQ_DELAY_CLOSE_TOPIC,
        consumerGroup = MQConstants.ROCKETMQ_DELAY_CLOSE_GROUP,
        consumeThreadNumber = 8,
        consumeTimeout = 15000L,
        maxReconsumeTimes = 3  // 延迟关单本身重试3次即可
)
@RequiredArgsConstructor
public class DelayCloseRocketMQConsumer implements RocketMQListener<String> {

    private static final int PAY_STATUS_WAITING = 1;//未付款
    private static final int PAY_STATUS_SUCCESS = 2;//订单的状态，1、未付款 2、已付款,未发货 3、已发货,未确认 4、确认收货，交易成功 5、交易取消，订单关闭 6、交易结束，已评价
    private static final int MAX_RECHECK_TIMES  = 3;//最大重试次数

    private final IOrderService orderService;
    private final IOrderDetailService orderDetailService;
    private final PayClient payClient;
    private final RocketMQTemplate rocketMQTemplate;
    private final StringRedisTemplate stringRedisTemplate;

    @Override
    public void onMessage(String msgJson) {
        JSONObject json = JSON.parseObject(msgJson);
        Long orderId    = json.getLong("orderId");
        int recheckTimes = json.getIntValue("recheckTimes");

        log.info("[延迟关单] 开始处理。orderId={}，recheckTimes={}", orderId, recheckTimes);

        try {
            // 1. 查询订单
            Order order = orderService.getById(orderId);

            if (order == null) {
                handleOrderNotFound(orderId, recheckTimes);
                return;
            }

            // 2. 订单状态不是未支付，直接放行
            if (OrderStatusEnum.getByCode(order.getStatus()) != OrderStatusEnum.UNPAID) {
                log.info("[延迟关单] 订单状态非未支付，放行。orderId={}", orderId);
                return;
            }

            // 3. 查询支付状态
            PayOrderDTO payOrder = payClient.queryPayOrderByBizOrderNo(orderId);
            Integer payStatus = payOrder == null ? null : payOrder.getStatus();

            if (Integer.valueOf(PAY_STATUS_SUCCESS).equals(payStatus)) {
                // 支付中心已支付，同步订单状态
                orderService.markOrderPaySuccess(orderId);
                log.info("[延迟关单] 订单实际已支付，更新状态成功。orderId={}", orderId);
                return;
            }

            if (needRecheck(payStatus, recheckTimes)) {
                // 支付状态未知，10 秒后复查（level 3）
                sendRecheckMessage(orderId, recheckTimes + 1, 3);
                log.warn("[延迟关单] 支付状态未知，10秒后复查。orderId={}，payStatus={}", orderId, payStatus);
                return;
            }

            // 4. 确认超时未支付，执行关单
            List<OrderDetail> detailList = orderDetailService.lambdaQuery()
                    .eq(OrderDetail::getOrderId, orderId).list();
            List<OrderDetailDTO> details = BeanUtils.copyList(detailList, OrderDetailDTO.class);
            orderService.cancelOrderAndRestore(orderId, details);
            log.info("[延迟关单] 超时关单成功。orderId={}，payStatus={}", orderId, payStatus);

        } catch (Exception e) {
            log.error("[延迟关单] 处理异常，触发重试。orderId={}", orderId, e);
            throw new RuntimeException("延迟关单处理失败", e);
        }
    }

    private void handleOrderNotFound(Long orderId, int recheckTimes) {
        String flagKey = RedisConstants.LUA_ORDER_FLAG_PREFIX + orderId;
        Boolean flagExists = stringRedisTemplate.hasKey(flagKey);

        if (Boolean.TRUE.equals(flagExists)) {
            // flag 存在：建单 consumer 还在重试中（最多2分钟），延迟3分钟后复查（level 7）
            sendRecheckMessage(orderId, recheckTimes + 1, 7);
            log.warn("[延迟关单] 订单不存在但flag存在，3分钟后复查。orderId={}", orderId);
        } else {
            // flag 不存在：DLQ 已完成逆向补偿，安全退出
            log.warn("[延迟关单] 订单不存在且flag不存在，建单已死，退出。orderId={}", orderId);
        }
    }

    private boolean needRecheck(Integer payStatus, int recheckTimes) {
        if (recheckTimes >= MAX_RECHECK_TIMES) return false;
        return payStatus == null || Integer.valueOf(PAY_STATUS_WAITING).equals(payStatus);
    }

    /**
     * 发送 RocketMQ 延迟复查消息。
     *
     * @param delayLevel RocketMQ 延迟级别（3=10s支付状态复查，7=3min建单复查，15=20min主延迟关单）
     */
    private void sendRecheckMessage(Long orderId, int recheckTimes, int delayLevel) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("orderId", orderId);
        payload.put("recheckTimes", recheckTimes);

        rocketMQTemplate.syncSend(
                MQConstants.ROCKETMQ_DELAY_CLOSE_TOPIC,
                MessageBuilder.withPayload(JSON.toJSONString(payload)).build(),
                3000,
                delayLevel
        );
    }
}