package com.hmall.item.domain.po;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 库存扣减幂等流水表。
 * <p>
 * 当前只承担“订单是否已扣减/是否已回滚”的幂等状态机职责，
 * 不再承载 MySQL 或 Redis 的版本事实。
 */
@Data
@TableName("stock_deduct_log")
public class StockDeductLog {
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;

    /** 业务订单号 (数据库中必须建 UNIQUE 唯一索引) */
    private Long orderId;

    /** 流水状态：1-已扣减，2-已回滚(为延迟关单退库存提供幂等兜底) */
    private Integer status;

    private LocalDateTime createTime;
}
