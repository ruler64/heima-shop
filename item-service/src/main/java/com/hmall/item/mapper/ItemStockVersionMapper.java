package com.hmall.item.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.hmall.item.domain.po.ItemStockVersion;
import org.apache.ibatis.annotations.Select;

public interface ItemStockVersionMapper extends BaseMapper<ItemStockVersion> {

    /**
     * 查询所有商品中最大的 MySQL epoch。
     * 返回 null 表示表中无数据（首次启动）。
     */
    @Select("SELECT MAX(mysql_epoch) FROM item_stock_version")
    Long selectMaxMysqlEpoch();
}
