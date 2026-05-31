package com.hmall.api.dto;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

/**
 * 订单明细 DTO（对外响应）
 * 对应 ES nested 子文档 OrderDetailDoc，与 order_detail 表字段一致
 * 位置：hm-api/dto/（与 ItemDTO 同包，方便各服务共用）
 */
@Data
@ApiModel(description = "订单明细")
public class EsOrderDetailDTO {

    @ApiModelProperty("SKU 商品 ID")
    private Long itemId;

    @ApiModelProperty("商品标题")
    private String name;

    @ApiModelProperty("购买数量")
    private Integer num;

    @ApiModelProperty("单价（分）")
    private Integer price;

    @ApiModelProperty("商品主图 URL")
    private String image;

    @ApiModelProperty("动态属性（颜色/尺码等）")
    private String spec;
}