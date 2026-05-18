package com.hmall.trade.domain.dto;

import com.hmall.api.dto.OrderDetailDTO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 取消订单事务执行上下文，由 cancelOrderAndRestore 构建，传入 RocketMQ 事务回调。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CancelExecutionContext {

    /**
     * 订单 ID
     */
    private Long orderId;

    /**
     * 订单明细，用于通知 item-service 恢复 MySQL 库存
     */
    private List<OrderDetailDTO> details;
}