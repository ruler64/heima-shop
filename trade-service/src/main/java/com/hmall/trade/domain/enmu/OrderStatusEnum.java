package com.hmall.trade.domain.enmu;

import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

/**
 * 订单状态枚举
 * 面试官考点：规范的常量管理，避免魔法值
 */
@Getter
public enum OrderStatusEnum {
    UNPAID(1, "未支付"),
    PAID(2, "已付款，未发货"),
    DELIVERED(3, "已发货，未确认"),
    COMPLETED(4, "确认收货，交易成功"),
    CLOSED(5, "已关闭(超时未支付)"),
    CANCELLED(6, "已取消");

    private final int code;
    private final String desc;
    private static final Map<Integer, OrderStatusEnum> LOOKUP = new HashMap<>();
    static {
        for (OrderStatusEnum status : values()) {
            LOOKUP.put(status.getCode(), status);
        }
    }
    OrderStatusEnum(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    /**
     * 将支付状态从“魔数”转为enum
     * @param code
     * @return
     */
    public static OrderStatusEnum getByCode(int code) {
        return LOOKUP.get(code); // O(1) 查询，比循环快
    }
}
