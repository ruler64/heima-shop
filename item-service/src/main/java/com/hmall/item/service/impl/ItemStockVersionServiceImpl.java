package com.hmall.item.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmall.item.domain.po.ItemStockVersion;
import com.hmall.item.mapper.ItemStockVersionMapper;
import com.hmall.item.service.IItemStockVersionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Slf4j
public class ItemStockVersionServiceImpl extends ServiceImpl<ItemStockVersionMapper, ItemStockVersion>
        implements IItemStockVersionService {

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void recordStockChange(Long itemId, Long orderId, String eventType) {
        ensureVersionRowExists(itemId, orderId, eventType);

        boolean updated = lambdaUpdate()
                .setSql("mysql_seq = mysql_seq + 1")
                .set(ItemStockVersion::getLastOrderId, orderId)
                .set(ItemStockVersion::getLastEventType, eventType)
                .eq(ItemStockVersion::getItemId, itemId)
                .update();
        if (!updated) {
            throw new IllegalStateException("更新 item_stock_version 失败, itemId=" + itemId);
        }
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void recordReconcileRepair(Long itemId) {
        ensureVersionRowExists(itemId, null, "RECONCILE");

        ItemStockVersion current = getOne(new LambdaQueryWrapper<ItemStockVersion>()
                .eq(ItemStockVersion::getItemId, itemId)
                .last("LIMIT 1"));
        if (current == null) {
            throw new IllegalStateException("查询 item_stock_version 失败, itemId=" + itemId);
        }

        boolean updated = lambdaUpdate()
                .set(ItemStockVersion::getMysqlEpoch, current.getMysqlEpoch() + 1)
                .set(ItemStockVersion::getMysqlSeq, 0L)
                .set(ItemStockVersion::getLastOrderId, null)
                .set(ItemStockVersion::getLastEventType, "RECONCILE")
                .eq(ItemStockVersion::getItemId, itemId)
                .eq(ItemStockVersion::getMysqlEpoch, current.getMysqlEpoch())
                .eq(ItemStockVersion::getMysqlSeq, current.getMysqlSeq())
                .update();
        if (!updated) {
            log.warn("item_stock_version 对账修复并发更新冲突，重试一次，itemId={}", itemId);
            recordReconcileRepairRetry(itemId);
        }
    }

    private void ensureVersionRowExists(Long itemId, Long orderId, String eventType) {
        ItemStockVersion version = new ItemStockVersion();
        version.setItemId(itemId);
        version.setMysqlEpoch(1L);
        version.setMysqlSeq(0L);
        version.setLastOrderId(orderId);
        version.setLastEventType(eventType);
        try {
            save(version);
        } catch (DuplicateKeyException e) {
            log.debug("item_stock_version 已存在，跳过初始化，itemId={}", itemId);
        }
    }

    private void recordReconcileRepairRetry(Long itemId) {
        ItemStockVersion current = getOne(new LambdaQueryWrapper<ItemStockVersion>()
                .eq(ItemStockVersion::getItemId, itemId)
                .last("LIMIT 1"));
        if (current == null) {
            throw new IllegalStateException("查询 item_stock_version 失败, itemId=" + itemId);
        }

        boolean updated = lambdaUpdate()
                .set(ItemStockVersion::getMysqlEpoch, current.getMysqlEpoch() + 1)
                .set(ItemStockVersion::getMysqlSeq, 0L)
                .set(ItemStockVersion::getLastOrderId, null)
                .set(ItemStockVersion::getLastEventType, "RECONCILE")
                .eq(ItemStockVersion::getItemId, itemId)
                .eq(ItemStockVersion::getMysqlEpoch, current.getMysqlEpoch())
                .eq(ItemStockVersion::getMysqlSeq, current.getMysqlSeq())
                .update();
        if (!updated) {
            throw new IllegalStateException("更新 item_stock_version 对账修复版本失败, itemId=" + itemId);
        }
    }
}
