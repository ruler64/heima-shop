package com.hmall.trade.task;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.hmall.api.dto.OrderDetailDTO;
import com.hmall.common.utils.BeanUtils;
import com.hmall.trade.domain.enmu.OrderStatusEnum;
import com.hmall.trade.domain.po.Order;
import com.hmall.trade.domain.po.OrderDetail;
import com.hmall.trade.service.IOrderDetailService;
import com.hmall.trade.service.IOrderService;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 利用MySQL中的订单表，XXL-Job 扫订单主表（每分钟执行）
 *         DB 恢复后 → 捞到超时未取消订单 → 重新触发 cancelOrderAndRestore
 *         cancelOrderAndRestore 重新发半事务消息 → 走回第一层
 *         最终收敛
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class cancelOverdueOrders {
    private final IOrderService orderService;
    private final IOrderDetailService detailService;

    @XxlJob("cancelOverdueOrderJob")
    public void cancelOverdueOrders() {
        // 直接扫订单主表：超过15分钟仍未支付的订单
        // 这个查询不依赖任何 outbox 表，数据库恢复后立刻能捞到数据
        LocalDateTime deadline = LocalDateTime.now().minusMinutes(15);

        List<Order> overdueOrders = orderService.lambdaQuery()
                .eq(Order::getStatus, OrderStatusEnum.UNPAID.getCode())
                .lt(Order::getCreateTime, deadline)
                .last("LIMIT 100")
                .list();

        if (overdueOrders == null || overdueOrders.isEmpty()) return;

        for (Order order : overdueOrders) {
            try {
                LambdaQueryWrapper<OrderDetail> queryWrapper = new LambdaQueryWrapper<>();
                queryWrapper.eq(OrderDetail::getOrderId, order.getId());
                List<OrderDetail> orderDetails = detailService.list(queryWrapper);
                if (orderDetails == null || orderDetails.isEmpty()) {
                    return;
                }

                List<OrderDetailDTO> details = orderDetails.stream()
                        .map(detail -> BeanUtils.copyProperties(detail, OrderDetailDTO.class))
                        .collect(Collectors.toList());
//                List<OrderDetailDTO> details = detailService.getDetailsByOrderId(order.getId());
                // cancelOrderAndRestore 内部有幂等保护，重复执行安全
                orderService.cancelOrderAndRestore(order.getId(), details);
                log.info("[兜底任务] 超时订单取消成功。orderId={}", order.getId());
            } catch (Exception e) {
                // 单个订单失败不影响其他订单，下次调度继续处理
                log.error("[兜底任务] 超时订单取消失败，下次重试。orderId={}", order.getId(), e);
            }
        }
    }
}
