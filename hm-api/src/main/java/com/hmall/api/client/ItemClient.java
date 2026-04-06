package com.hmall.api.client;


import com.hmall.api.client.fallback.ItemClientFallbackFactory;
import com.hmall.api.dto.ItemDTO;
import com.hmall.api.dto.OrderDetailDTO;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;
import java.util.List;

@FeignClient(value = "item-service", fallbackFactory = ItemClientFallbackFactory.class)
public interface ItemClient {

    @GetMapping("/items")
    List<ItemDTO> queryItemByIds(@RequestParam("ids") Collection<Long> ids);

    @PutMapping("/items/stock/deduct")
    public void deductStock(Long orderId,@RequestBody List<OrderDetailDTO> items);

    @PutMapping("/items/stock/increase")
    public void increaseStock(Long orderId,@RequestBody List<OrderDetailDTO> items);

    @GetMapping("{/items/id}")
    public ItemDTO queryItemById(@PathVariable("id") Long id);
}
