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
    public void deductStock(@RequestParam("orderId")Long orderId,@RequestBody List<OrderDetailDTO> items);

    @PutMapping("/items/stock/increase")
    public void increaseStock(@RequestParam("orderId")Long orderId,@RequestBody List<OrderDetailDTO> items);

    @GetMapping("/items/{id}")
    public ItemDTO queryItemById(@PathVariable("id") Long id);

    /**
     * 懒加载单个商品库存到 Redis。
     *
     * <p>trade-service 在 Lua 扣减时发现某商品库存 key 缺失（返回负数 index）时调用。
     * item-service 从 MySQL 读取当前库存，以短 TTL 写入 Redis，
     * 供 trade-service 重试 Lua 使用；后续凌晨对账会接管长期同步。
     *
     * @param itemId 商品 ID
     */
    @PostMapping("/items/stock/load/{itemId}")
    void loadStockToRedis(@PathVariable("itemId") Long itemId);
}
