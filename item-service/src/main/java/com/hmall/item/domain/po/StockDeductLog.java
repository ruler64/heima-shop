package com.hmall.item.domain.po;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import java.time.LocalDateTime;

/**
 * 库存扣减流水表 (用于实现分布式幂等)
 */
@Data
@TableName("stock_deduct_log")
public class StockDeductLog {
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;

    /** 业务订单号 (数据库中必须建 UNIQUE 唯一索引) */
    private Long orderId;

    /** 流水状态：1-已扣减，2-已回滚(为将来延迟关单退库存留后路) */
    private Integer status;

    private LocalDateTime createTime;
}