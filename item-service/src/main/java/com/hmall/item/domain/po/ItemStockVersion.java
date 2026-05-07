package com.hmall.item.domain.po;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 商品库存 MySQL 侧版本状态表。
 * <p>
 * 这是 MySQL 库存事实源的版本，不从 Redis epoch/seq 派生。
 */
@Data
@TableName("item_stock_version")
public class ItemStockVersion {

    @TableId(type = IdType.AUTO)
    private Long id;

    /** 商品 ID，唯一索引 */
    private Long itemId;

    /** MySQL 库存世代号，只有 MySQL 事实源重建/人工修复时递增 */
    private Long mysqlEpoch;

    /** MySQL 侧单商品库存变更递增序号 */
    private Long mysqlSeq;

    /** 最近一次影响库存的订单 ID */
    private Long lastOrderId;

    /** 最近事件类型：DEDUCT/RESTORE/RECONCILE */
    private String lastEventType;

    private LocalDateTime createTime;

    private LocalDateTime updateTime;
}
