package com.hmall.trade.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.hmall.api.dto.ItemDTO;
import com.hmall.api.dto.OrderDetailDTO;
import com.hmall.trade.domain.dto.OrderFormDTO;
import com.hmall.trade.domain.po.Order;

import java.util.List;
import java.util.Map;


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

    void preHandleOrder(Long orderId, Long userId, OrderFormDTO orderFormDTO);

    void handleDbOrder(Long orderId, Long userId, OrderFormDTO orderFormDTO,
                       List<ItemDTO> items, Map<Long, Integer> itemNumMap, int total);//Long createOrder

    void cancelOrderWithOutbox(Long orderId, List<OrderDetailDTO> details);

    void markOrderPaySuccess(Long orderId);

    void cancelOrder(Long orderId);
    // 新增：分布式事务逆向回滚专用方法
    //void cancelOrderAndRestore(Long orderId, List<OrderDetailDTO> details);

    //boolean updateStatusToCancelled(Long orderId);
}
