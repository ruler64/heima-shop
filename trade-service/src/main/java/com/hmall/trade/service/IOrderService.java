package com.hmall.trade.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.hmall.api.dto.OrderDetailDTO;
import com.hmall.trade.domain.dto.OrderFormDTO;
import com.hmall.trade.domain.po.Order;

import java.util.List;


/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2025-05-05
 */
public interface IOrderService extends IService<Order> {
    Long createOrder(OrderFormDTO orderFormDTO);//Long createOrder

    void handleDbOrder(Long orderId,Long userId,OrderFormDTO orderFormDTO);//Long createOrder

    void markOrderPaySuccess(Long orderId);

    void cancelOrder(Long orderId);
    // 新增：分布式事务逆向回滚专用方法
    void cancelOrderAndRestore(Long orderId, List<OrderDetailDTO> details);
}
