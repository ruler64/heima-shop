package com.hmall.search.domain.doc;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;

/**
 * ES 文档：order_index
 *
 * 路由策略：routing = userId（C 端查询命中单 shard，商户查询广播）
 * _id = orderId（保证幂等覆盖）
 *
 * 不继承任何 MybatisPlus 注解，仅作为 ES JSON 序列化载体。
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class OrderDoc {

    /** 订单 ID，同时作为 ES _id */
    private Long orderId;

    /** 用户 ID，routing 字段 */
    private Long userId;

    /** 订单状态：1未付款 2已付款 3已发货 4确认收货 5已取消 6已评价 */
    private Integer status;

    /** 支付金额，单位：分 */
    private Integer totalFee;

    /** 支付方式：1支付宝 2微信 3余额 */
    private Integer paymentType;

    private LocalDateTime createTime;
    private LocalDateTime payTime;
    private LocalDateTime consignTime;
    private LocalDateTime endTime;
    private LocalDateTime closeTime;

    /** 订单明细，ES nested 类型 */
    private List<OrderDetailDoc> items;
}