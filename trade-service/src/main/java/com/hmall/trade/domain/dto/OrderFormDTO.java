package com.hmall.trade.domain.dto;

import com.hmall.api.dto.OrderDetailDTO;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.List;

@Data
@ApiModel(description = "交易下单表单实体")
public class OrderFormDTO implements Serializable {
    private static final long serialVersionUID = 1L; // 加上这个好习惯
    @NotNull(message = "收货地址不能为空")
    @ApiModelProperty("收货地址id")
    private Long addressId;
    
    @NotNull(message = "支付类型不能为空")
    @ApiModelProperty("支付类型")
    private Integer paymentType;

    @Valid
    @NotEmpty(message = "下单商品不能为空")
    @ApiModelProperty("下单商品列表")
    private List<OrderDetailDTO> details;
}
