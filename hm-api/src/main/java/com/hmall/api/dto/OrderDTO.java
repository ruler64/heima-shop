package com.hmall.api.dto;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 订单 DTO（对外响应）
 * 对应 ES 文档 OrderDoc，屏蔽 ES 内部实现，解耦前端与存储层
 * 位置：hm-api/dto/（与 ItemDTO 同包）
 */
@Data
@ApiModel(description = "订单信息")
public class OrderDTO {

    @ApiModelProperty("订单 ID")
    private Long orderId;

    @ApiModelProperty("用户 ID")
    private Long userId;

    @ApiModelProperty("订单状态：1未付款 2已付款 3已发货 4确认收货 5已取消 6已评价")
    private Integer status;

    @ApiModelProperty("总金额（分）")
    private Integer totalFee;

    @ApiModelProperty("支付方式：1支付宝 2微信 3余额")
    private Integer paymentType;

    @ApiModelProperty("下单时间")
    private LocalDateTime createTime;

    @ApiModelProperty("支付时间")
    private LocalDateTime payTime;

    @ApiModelProperty("发货时间")
    private LocalDateTime consignTime;

    @ApiModelProperty("交易完成时间")
    private LocalDateTime endTime;

    @ApiModelProperty("关单时间")
    private LocalDateTime closeTime;

    @ApiModelProperty("订单明细列表")
    private List items;

    // ── 商户查询预留字段 ──────────────────────────────────────────
    // 待 order 表增加 merchant_id 列、OrderDoc / Mapping 补充后启用
    // @ApiModelProperty("商户 ID（B端查询用）")
    // private Long merchantId;
}