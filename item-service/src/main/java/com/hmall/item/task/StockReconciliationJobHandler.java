package com.hmall.item.task;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.hmall.item.constants.StockVersionConstants;
import com.hmall.item.domain.po.Item;
import com.hmall.item.domain.po.ItemStockVersion;
import com.hmall.item.domain.po.StockReconcileState;
import com.hmall.item.service.IItemService;
import com.hmall.item.service.IItemStockVersionService;
import com.hmall.item.service.IStockReconcileStateService;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 库存对账与收敛任务。
 * <p>
 * 大原则：MySQL item + item_stock_version 是事实源；Redis 库存和 Redis version 只是待校验缓存状态。
 * <p>
 * 状态流转：
 * 0-观察中：存在差异，进入退避观察窗口；
 * 1-已修复：确认 Redis 不安全或连续观察后，由 MySQL 事实源回填完成；
 * 2-忽略：短暂差异已自然收敛，无需修复；
 * 3-人工介入：发现无法自动判定或多轮修复失败，需要人工排查。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StockReconciliationJobHandler {

    private static final int MAX_BACKOFF_SECONDS = 300;
    private static final int STATUS_OBSERVING = 0;
    private static final int STATUS_REPAIRED = 1;
    private static final int STATUS_IGNORED = 2;
    private static final int STATUS_MANUAL = 3;

    private final IItemService itemService;
    private final StringRedisTemplate stringRedisTemplate;
    private final IItemStockVersionService itemStockVersionService;
    private final IStockReconcileStateService stockReconcileStateService;

    @XxlJob("stockReconciliationJob")
    public void execute() {
        XxlJobHelper.log("库存对账任务启动：以 MySQL item + item_stock_version 为事实源...");

        int pageSize = 500;
        long lastId = 0L;
        int diffCount = 0;
        int repairedCount = 0;
        int ignoredCount = 0;
        int manualCount = 0;

        while (true) {
            List<Item> items = itemService.list(new LambdaQueryWrapper<Item>()
                    .gt(Item::getId, lastId)
                    .eq(Item::getStatus, 1)
                    .orderByAsc(Item::getId)
                    .last("LIMIT " + pageSize));

            if (items == null || items.isEmpty()) {
                break;
            }

            Map<String, String> repairMap = new HashMap<>();
            for (Item item : items) {
                lastId = item.getId();
                ItemStockVersion mysqlVersion = getOrInitMysqlVersion(item.getId());
                String stockKey = StockVersionConstants.STOCK_KEY_PREFIX + item.getId();
                String versionKey = StockVersionConstants.STOCK_VERSION_KEY_PREFIX + item.getId();

                RedisStockSnapshot redisSnapshot = readRedisSnapshot(stockKey, versionKey);
                String diffType = resolveDiffType(item, mysqlVersion, redisSnapshot);
                StockReconcileState state = getOrInitReconcileState(item.getId());

                if (diffType == null) {
                    if (state.getStatus() != null && state.getStatus() == STATUS_OBSERVING && state.getRetryCount() != null && state.getRetryCount() > 0) {
                        ignoredCount++;
                        markIgnored(item, mysqlVersion, redisSnapshot, state, "DIFF_AUTO_CONVERGED");
                    } else {
                        clearStateIfPresent(item.getId());
                    }
                    continue;
                }

                diffCount++;
                XxlJobHelper.log("库存差异，itemId={}, diffType={}, mysqlStock={}, redisStock={}, mysqlVer={}|{}, redisVer={}|{}",
                        item.getId(), diffType, item.getStock(), redisSnapshot.redisStock,
                        mysqlVersion.getMysqlEpoch(), mysqlVersion.getMysqlSeq(), redisSnapshot.redisEpoch, redisSnapshot.redisSeq);

                if (isManualRequired(diffType, state)) {
                    manualCount++;
                    markManual(item, mysqlVersion, redisSnapshot, state, diffType);
                    continue;
                }

                if (isUnsafeDiff(diffType)) {
                    itemStockVersionService.recordReconcileRepair(item.getId());
                    ItemStockVersion repairedVersion = getOrInitMysqlVersion(item.getId());
                    putRepairMap(repairMap, stockKey, versionKey, item, repairedVersion);
                    markRepaired(item, repairedVersion, redisSnapshot, state, diffType);
                    repairedCount++;
                    XxlJobHelper.log("发现 Redis 库存不安全，立即按 MySQL 事实源回填，itemId={}, diffType={}", item.getId(), diffType);
                    continue;
                }

                if (shouldIgnoreImmediately(diffType)) {
                    ignoredCount++;
                    markIgnored(item, mysqlVersion, redisSnapshot, state, diffType);
                    continue;
                }

                BackoffDecision decision = observeAndDecide(item, mysqlVersion, redisSnapshot, state, diffType);
                if (decision.status == STATUS_REPAIRED) {
                    itemStockVersionService.recordReconcileRepair(item.getId());
                    ItemStockVersion repairedVersion = getOrInitMysqlVersion(item.getId());
                    putRepairMap(repairMap, stockKey, versionKey, item, repairedVersion);
                    markRepaired(item, repairedVersion, redisSnapshot, state, diffType);
                    repairedCount++;
                    XxlJobHelper.log("连续观察后满足收敛条件，按 MySQL 事实源回填 Redis，itemId={}, diffType={}", item.getId(), diffType);
                } else if (decision.status == STATUS_MANUAL) {
                    manualCount++;
                    markManual(item, mysqlVersion, redisSnapshot, state, diffType);
                }
            }

            if (!repairMap.isEmpty()) {
                stringRedisTemplate.opsForValue().multiSet(repairMap);
                XxlJobHelper.log("本批次已修复 Redis 库存 {} 项", repairMap.size() / 2);
            }
        }

        XxlJobHelper.log("库存对账任务完成，累计差异商品数={}，已收敛修复={} 项，已忽略={} 项，人工介入={} 项",
                diffCount, repairedCount, ignoredCount, manualCount);
    }

    private ItemStockVersion getOrInitMysqlVersion(Long itemId) {
        ItemStockVersion version = itemStockVersionService.getOne(new LambdaQueryWrapper<ItemStockVersion>()
                .eq(ItemStockVersion::getItemId, itemId)
                .last("LIMIT 1"));
        if (version != null) {
            return version;
        }
        itemStockVersionService.recordStockChange(itemId, null, "INIT");
        ItemStockVersion initVersion = itemStockVersionService.getOne(new LambdaQueryWrapper<ItemStockVersion>()
                .eq(ItemStockVersion::getItemId, itemId)
                .last("LIMIT 1"));
        if (initVersion == null) {
            throw new IllegalStateException("初始化 item_stock_version 失败, itemId=" + itemId);
        }
        return initVersion;
    }

    private StockReconcileState getOrInitReconcileState(Long itemId) {
        StockReconcileState state = stockReconcileStateService.getOne(new LambdaQueryWrapper<StockReconcileState>()
                .eq(StockReconcileState::getItemId, itemId)
                .last("LIMIT 1"));
        if (state != null) {
            return state;
        }
        state = new StockReconcileState();
        state.setItemId(itemId);
        state.setRetryCount(0);
        state.setStatus(STATUS_IGNORED);
        stockReconcileStateService.save(state);
        return state;
    }

    private void clearStateIfPresent(Long itemId) {
        stockReconcileStateService.remove(new LambdaQueryWrapper<StockReconcileState>()
                .eq(StockReconcileState::getItemId, itemId));
    }

    private RedisStockSnapshot readRedisSnapshot(String stockKey, String versionKey) {
        RedisStockSnapshot snapshot = new RedisStockSnapshot();
        snapshot.redisStockStr = stringRedisTemplate.opsForValue().get(stockKey);
        snapshot.redisVersion = stringRedisTemplate.opsForValue().get(versionKey);

        try {
            snapshot.redisStock = snapshot.redisStockStr == null ? null : Integer.valueOf(snapshot.redisStockStr);
        } catch (Exception ignore) {
            snapshot.redisStock = null;
        }

        if (snapshot.redisVersion != null && snapshot.redisVersion.contains(StockVersionConstants.VERSION_SEPARATOR)) {
            String[] parts = snapshot.redisVersion.split("\\|", 2);
            if (parts.length == 2) {
                try {
                    snapshot.redisEpoch = Long.valueOf(parts[0]);
                    snapshot.redisSeq = Long.valueOf(parts[1]);
                } catch (Exception ignore) {
                    snapshot.redisEpoch = null;
                    snapshot.redisSeq = null;
                }
            }
        }
        return snapshot;
    }

    private String resolveDiffType(Item item, ItemStockVersion mysqlVersion, RedisStockSnapshot redisSnapshot) {
        if (redisSnapshot.redisStock == null) {
            return "REDIS_STOCK_MISSING_OR_INVALID";
        }
        if (redisSnapshot.redisEpoch == null || redisSnapshot.redisSeq == null) {
            return "REDIS_VERSION_MISSING_OR_INVALID";
        }
        if (redisSnapshot.redisStock > item.getStock()) {
            return "REDIS_STOCK_GT_MYSQL";
        }
        if (redisSnapshot.redisEpoch > mysqlVersion.getMysqlEpoch()
                && redisSnapshot.redisSeq < mysqlVersion.getMysqlSeq()) {
            return "REDIS_SEQ_ROLLBACK_AFTER_EPOCH_CHANGE";
        }
        if (!item.getStock().equals(redisSnapshot.redisStock)) {
            return "STOCK_NOT_EQUAL_OBSERVE";
        }
        return null;
    }

    private boolean isUnsafeDiff(String diffType) {
        return "REDIS_STOCK_GT_MYSQL".equals(diffType)
                || "REDIS_SEQ_ROLLBACK_AFTER_EPOCH_CHANGE".equals(diffType);
    }

    private boolean shouldIgnoreImmediately(String diffType) {
        return false;
    }

    private boolean isManualRequired(String diffType, StockReconcileState state) {
        return state.getRetryCount() != null && state.getRetryCount() >= 5
                && ("REDIS_STOCK_MISSING_OR_INVALID".equals(diffType)
                || "REDIS_VERSION_MISSING_OR_INVALID".equals(diffType));
    }

    private BackoffDecision observeAndDecide(Item item, ItemStockVersion mysqlVersion,
                                             RedisStockSnapshot redisSnapshot, StockReconcileState state,
                                             String diffType) {
        int retryCount = state.getRetryCount() == null ? 0 : state.getRetryCount();
        retryCount++;
        int backoffSeconds = Math.min((1 << Math.min(retryCount, 8)), MAX_BACKOFF_SECONDS);
        LocalDateTime nextRetryAt = LocalDateTime.now().plusSeconds(backoffSeconds);

        fillState(state, item, mysqlVersion, redisSnapshot, diffType, retryCount, nextRetryAt, STATUS_OBSERVING);
        stockReconcileStateService.updateById(state);

        if (retryCount >= 3) {
            return new BackoffDecision(STATUS_REPAIRED);
        }
        return new BackoffDecision(STATUS_OBSERVING);
    }

    private void markRepaired(Item item, ItemStockVersion mysqlVersion,
                              RedisStockSnapshot redisSnapshot, StockReconcileState state, String diffType) {
        fillState(state, item, mysqlVersion, redisSnapshot, diffType,
                state.getRetryCount() == null ? 0 : state.getRetryCount(), null, STATUS_REPAIRED);
        stockReconcileStateService.updateById(state);
    }

    private void markIgnored(Item item, ItemStockVersion mysqlVersion,
                             RedisStockSnapshot redisSnapshot, StockReconcileState state, String diffType) {
        fillState(state, item, mysqlVersion, redisSnapshot, diffType,
                state.getRetryCount() == null ? 0 : state.getRetryCount(), null, STATUS_IGNORED);
        stockReconcileStateService.updateById(state);
    }

    private void markManual(Item item, ItemStockVersion mysqlVersion,
                            RedisStockSnapshot redisSnapshot, StockReconcileState state, String diffType) {
        fillState(state, item, mysqlVersion, redisSnapshot, diffType,
                state.getRetryCount() == null ? 0 : state.getRetryCount(), null, STATUS_MANUAL);
        stockReconcileStateService.updateById(state);
    }

    private void putRepairMap(Map<String, String> repairMap, String stockKey, String versionKey,
                              Item item, ItemStockVersion mysqlVersion) {
        repairMap.put(stockKey, String.valueOf(item.getStock()));
        repairMap.put(versionKey, mysqlVersion.getMysqlEpoch()
                + StockVersionConstants.VERSION_SEPARATOR
                + mysqlVersion.getMysqlSeq());
    }

    private void fillState(StockReconcileState state, Item item, ItemStockVersion mysqlVersion,
                           RedisStockSnapshot redisSnapshot, String diffType,
                           int retryCount, LocalDateTime nextRetryAt, int status) {
        state.setMysqlStock(item.getStock());
        state.setRedisStock(redisSnapshot.redisStock);
        state.setMysqlEpoch(mysqlVersion.getMysqlEpoch());
        state.setMysqlSeq(mysqlVersion.getMysqlSeq());
        state.setRedisEpoch(redisSnapshot.redisEpoch);
        state.setRedisSeq(redisSnapshot.redisSeq);
        state.setDiffType(diffType);
        state.setRetryCount(retryCount);
        state.setNextRetryAt(nextRetryAt);
        state.setStatus(status);
        state.setLastCheckTime(LocalDateTime.now());
    }

    private static class RedisStockSnapshot {
        private String redisStockStr;
        private String redisVersion;
        private Integer redisStock;
        private Long redisEpoch;
        private Long redisSeq;
    }

    private static class BackoffDecision {
        private final int status;

        private BackoffDecision(int status) {
            this.status = status;
        }
    }
}
