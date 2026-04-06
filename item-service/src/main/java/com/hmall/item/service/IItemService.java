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

    void deductStock(Long orderId,List<OrderDetailDTO> items);

    PageDTO<ItemDTO> queryItemByPageWithCache(PageQuery query);

    List<ItemDTO> queryItemByIds(Collection<Long> ids);

    void increaseStock(Long orderId, List<OrderDetailDTO> items);

}
