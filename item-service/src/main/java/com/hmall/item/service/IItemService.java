package com.hmall.item.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.hmall.api.dto.ItemDTO;
import com.hmall.api.dto.OrderDetailDTO;
import com.hmall.common.domain.PageDTO;
import com.hmall.common.domain.PageQuery;
import com.hmall.item.domain.po.Item;
import java.util.Collection;
import java.util.List;

/**
 * <p>
 * 商品表 服务类
 * </p>
 *
 * @author 虎哥
 * @since 2023-05-05
 */
public interface IItemService extends IService<Item> {

    void deductStock(Long orderId, List<OrderDetailDTO> items);

    PageDTO<ItemDTO> queryItemByPageWithCache(PageQuery query);

    List<ItemDTO> queryItemByIds(Collection<Long> ids);

    boolean increaseStock(Long orderId, List<OrderDetailDTO> items);

    void loadStockToRedis(Long itemId);

    /**
     * 批量懒加载：将多个商品的 MySQL 库存原子写入 Redis。
     * 供 trade-service 在 Lua 扣减发现 key 缺失时通过 Feign 调用。
     */
    void batchLoadStockToRedis(List<Long> itemIds);

}
