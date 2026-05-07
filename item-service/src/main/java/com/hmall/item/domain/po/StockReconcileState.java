package com.hmall.item.domain.po;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 库存对账退避状态表。
 * <p>
 * 用于替代 Redis hash 保存对账观察、退避和修复状态。
 */
@Data
@TableName("stock_reconcile_state")
public class StockReconcileState {

    @TableId(type = IdType.AUTO)
    private Long id;

    /** 商品 ID，唯一索引 */
    private Long itemId;

    /** MySQL 事实源库存 */
    private Integer mysqlStock;

    /** Redis 当前库存，可能为空或不可解析 */
    private Integer redisStock;

    /** MySQL 事实源 epoch */
    private Long mysqlEpoch;

    /** MySQL 事实源 seq */
    private Long mysqlSeq;

    /** Redis epoch，可能为空或不可解析 */
    private Long redisEpoch;

    /** Redis seq，可能为空或不可解析 */
    private Long redisSeq;

    /** 差异类型 */
    private String diffType;

    /** 指数退避重试次数 */
    private Integer retryCount;

    /** 下次允许处理时间 */
    private LocalDateTime nextRetryAt;

    /** 0-观察中,1-已修复,2-忽略,3-人工介入 */
    private Integer status;

    /** 最近检查时间 */
    private LocalDateTime lastCheckTime;

    private LocalDateTime createTime;

    private LocalDateTime updateTime;
}
