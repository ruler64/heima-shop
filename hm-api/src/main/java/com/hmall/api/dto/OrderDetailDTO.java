package com.hmall.api.dto;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.experimental.Accessors;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.io.Serializable;

@ApiModel(description = "订单明细条目")
@Data
@Accessors(chain = true)
public class OrderDetailDTO implements Serializable {
    private static final long serialVersionUID = 1L; // 加上这个好习惯

    @NotNull(message = "商品id不能为空")
    @ApiModelProperty("商品id")
    private Long itemId;

    @NotNull(message = "商品购买数量不能为空")
    @Min(value = 1, message = "商品购买数量必须大于0")
    @ApiModelProperty("商品购买数量")
    private Integer num;
}
