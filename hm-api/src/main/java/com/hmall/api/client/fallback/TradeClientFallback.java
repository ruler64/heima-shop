package com.hmall.api.client.fallback;

import com.hmall.api.client.TradeClient;
import com.hmall.api.dto.Order;
import com.hmall.common.utils.CollUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.openfeign.FallbackFactory;

import java.util.List;

@Slf4j
public class TradeClientFallback implements FallbackFactory<TradeClient> {
    @Override
    public TradeClient create(Throwable cause) {
        log.warn("TradeClient fallback triggered", cause);
        return new TradeClient() {

            @Override
            public Boolean existsPendingOutbox() {
                return true;
            }

            /*@Override
            public List<Order> getOrdersByUserIdFromDB(Long userId, int page, int size) {
                log.error("根据用户ID查询订单失败！",cause);
                return CollUtils.emptyList();
            }

            @Override
            public List<Order> getOrdersByMerchantIdFromDB(Long merchantId, int page, int size) {
                log.error("根据商户ID查询订单失败！",cause);
                return CollUtils.emptyList();
            }*/
        };
        //return () -> Boolean.TRUE;
    }
}
