package com.hmall.item.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.hmall.item.domain.po.ItemStockVersion;

public interface IItemStockVersionService extends IService<ItemStockVersion> {

    /**
     * 记录一次 MySQL 库存事实变更，和 item.stock 更新处于同一个本地事务。
     */
    void recordStockChange(Long itemId, Long orderId, String eventType);

    /**
     * 对账确认 Redis 不安全并以 MySQL 回填后，提升 MySQL epoch，表示进入新的事实世代。
     */
    void recordReconcileRepair(Long itemId);

    /**
     * 查询 MySQL 侧所有商品中最大的 mysqlEpoch。
     * 用于 Redis failover 后计算新 epoch = max + 1。
     * 若表为空返回 null（调用方应视作 epoch 0）。
     */
    Long getMaxMysqlEpoch();
}
