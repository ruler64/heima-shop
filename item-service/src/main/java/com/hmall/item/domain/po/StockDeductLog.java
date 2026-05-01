package com.hmall.item.domain.po;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 库存扣减流水表 (用于实现分布式幂等与对账)
 */
@Data
@TableName("stock_deduct_log")
public class StockDeductLog {
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;

    /** 业务订单号 (数据库中必须建 UNIQUE 唯一索引) */
    private Long orderId;

    /** 商品 ID，便于单品维度对账 */
    //private Long itemId;

    /** 扣减数量 */
    private Integer deductNum;

    /** Redis 故障世代号 */
    private Long epoch;

    /** 当前世代内单调递增序号 */
    private Long seq;

    /** 逻辑版本号，建议格式：epoch|seq */
    private String version;

    /** 流水状态：1-已扣减，2-已回滚(为将来延迟关单退库存留后路) */
    private Integer status;

    private LocalDateTime createTime;
}