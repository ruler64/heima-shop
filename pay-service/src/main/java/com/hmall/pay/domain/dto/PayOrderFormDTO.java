package com.hmall.pay.domain.dto;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Builder;
import lombok.Data;

import javax.validation.constraints.NotNull;

@Data
@Builder//自动生成一个静态内部类 XXXBuilder‌（如 StudentBuilder），其中包含与目标类字段对应的 setter 方法。PayOrderFormDTO.builder().id().pw()
@ApiModel(description = "支付确认表单实体")
public class PayOrderFormDTO {
    @ApiModelProperty("支付订单id不能为空")
    @NotNull(message = "支付订单id不能为空")
    private Long id;
    @ApiModelProperty("支付密码")
    @NotNull(message = "支付密码")
    private String pw;
}