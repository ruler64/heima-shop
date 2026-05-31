package com.hmall.api.client;

import com.hmall.api.client.fallback.TradeClientFallback;
import com.hmall.api.dto.Order;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;

@FeignClient(value = "trade-service", fallbackFactory = TradeClientFallback.class)
public interface TradeClient {

    /**
     * 查询交易服务是否仍存在未投递完成的本地消息。
     * 库存对账在该值为 true 时必须进入退避，避免 MQ 飞行消息尚未落地就误覆盖 Redis。
     */
    @GetMapping("/orders/pending-exists")
    Boolean existsPendingOutbox();

//    @GetMapping("/orders/user/{userId}")
//    List<EsOrderDoc> getOrdersByUserIdFromDB(@PathVariable Long userId, @RequestParam("page") int page, @RequestParam("size") int size);
//
//    @GetMapping("/orders/merchant/{merchantId}")
//    List<EsOrderDoc> getOrdersByMerchantIdFromDB(@PathVariable Long merchantId, @RequestParam("page") int page, @RequestParam("size") int size);
}
