package com.hmall.search.domain.doc;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ES nested 字段：订单明细子文档
 * 对应 order_detail 表，嵌入 OrderDoc.items 中
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class OrderDetailDoc {

    /** SKU 商品 ID */
    private Long   itemId;

    /** 商品标题（ik 分词，支持关键词搜索） */
    private String name;

    /** 购买数量 */
    private Integer num;

    /** 单价，单位：分 */
    private Integer price;

    /** 商品主图 URL */
    private String image;

    /** 动态属性键值集 */
    private String spec;
}