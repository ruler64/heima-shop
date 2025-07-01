package com.hmall.search.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.hmall.search.domain.po.Item;
import com.hmall.search.domain.query.ItemPageQuery;
import com.hmall.api.dto.ItemDTO;
import com.hmall.common.domain.PageDTO;


import java.io.IOException;
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
public interface ISearchService extends IService<Item> {

    List<ItemDTO> queryItemByIds(Collection<Long> ids);

    public PageDTO<ItemDTO> search(ItemPageQuery query) throws IOException;

}
