package com.hmall.item.task;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.hmall.api.client.TradeClient;
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
    private final TradeClient tradeClient;

    @XxlJob("stockReconciliationJob")
    public void execute() {
        XxlJobHelper.log("库存对账任务启动：以 MySQL item + item_stock_version 为事实源...");

        Boolean pendingOutbox = tradeClient.existsPendingOutbox();
        if (Boolean.TRUE.equals(pendingOutbox)) {
            XxlJobHelper.log("检测到 trade-service 本地消息表仍有 status=0 的未完成消息，本轮对账整体退避并告警");
            log.warn("库存对账跳过：trade-service local_event_outbox 存在未完成消息(status=0)");
            return;
        }

        int pageSize = 500;
        long lastId = 0L;
        int diffCount = 0; //累计差异商品数
        int repairedCount = 0; //已收敛修复={} 项
        int ignoredCount = 0; //已忽略={} 项
        int manualCount = 0; //人工介入={} 项

        while (true) {
            List<Item> items = itemService.list(new LambdaQueryWrapper<Item>()
                    .gt(Item::getId, lastId)
                    .eq(Item::getStatus, 1)
                    .orderByAsc(Item::getId)
                    .last("LIMIT " + pageSize));//游标法防止深分页灾难
            // 健壮性判断：商品集合是否为空
            if (items == null || items.isEmpty()) {
                break;
            }

            Map<String, String> repairMap = new HashMap<>();
            for (Item item : items) {
                lastId = item.getId();
                // 初始化mysql版本
                ItemStockVersion mysqlVersion = getOrInitMysqlVersion(item.getId());
                // 组装redis的库存和版本key
                String stockKey = StockVersionConstants.STOCK_KEY_PREFIX + item.getId();
                String versionKey = StockVersionConstants.STOCK_VERSION_KEY_PREFIX + item.getId();

                // 获取redis对应商品的库存数量和版本快照
                RedisStockSnapshot redisSnapshot = readRedisSnapshot(stockKey, versionKey);
                // 根据当前mysql版本和redis快照版本的状态->当前属于那个不同的状态
                String diffType = resolveDiffType(item, mysqlVersion, redisSnapshot);
                // 获取或初始化当前的恢复状态
                StockReconcileState state = getOrInitReconcileState(item.getId());

                // 若当前商品的redis与mysql的版本与库存数量对齐了
                if (diffType == null) {
                    if (state.getStatus() != null && state.getStatus() == STATUS_OBSERVING && state.getRetryCount() != null && state.getRetryCount() > 0) {
                        // 若之前存在差异进入了观察状态，此时又把差异对齐消除了，那么忽略当前商品
                        ignoredCount++;
                        markIgnored(item, mysqlVersion, redisSnapshot, state, "DIFF_AUTO_CONVERGED");
                    } else {
                        // 如果不存在差异，直接删除对应商品的恢复状态
                        clearStateIfPresent(item.getId());
                    }
                    continue;
                }
                // 若当前商品存在差异，打印日志
                diffCount++;
                XxlJobHelper.log("库存差异，itemId={}, diffType={}, mysqlStock={}, redisStock={}, mysqlVer={}|{}, redisVer={}|{}",
                        item.getId(), diffType, item.getStock(), redisSnapshot.redisStock,
                        mysqlVersion.getMysqlEpoch(), mysqlVersion.getMysqlSeq(), redisSnapshot.redisEpoch, redisSnapshot.redisSeq);

                // 若需要人工介入
                if (isManualRequired(diffType, state)) {
                    manualCount++;
                    markManual(item, mysqlVersion, redisSnapshot, state, diffType);
                    continue;
                }

                // 若redis出现宕机导致的库存和版本号不一致
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

    /**
     * 获取或初始化mysql对应商品的版本号
     * @param itemId 商品id
     * @return 返回商品库存版本
     */
    private ItemStockVersion getOrInitMysqlVersion(Long itemId) {
        // 有版本号则获取
        ItemStockVersion version = itemStockVersionService.getOne(new LambdaQueryWrapper<ItemStockVersion>()
                .eq(ItemStockVersion::getItemId, itemId)
                .last("LIMIT 1"));
        if (version != null) {
            return version;
        }
        // 无版本号则初始化——凌晨对账一般是有版本号的只要商品被买过（在购买商品后会递增版本号）
        itemStockVersionService.recordStockChange(itemId, null, "INIT");
        ItemStockVersion initVersion = itemStockVersionService.getOne(new LambdaQueryWrapper<ItemStockVersion>()
                .eq(ItemStockVersion::getItemId, itemId)
                .last("LIMIT 1"));
        if (initVersion == null) {
            throw new IllegalStateException("初始化 item_stock_version 失败, itemId=" + itemId);
        }
        return initVersion;
    }

    /**
     * 获取或初始化恢复状态
     * @param itemId 当前商品id
     * @return 返回库存恢复状态StockReconcileState
     */
    private StockReconcileState getOrInitReconcileState(Long itemId) {
        // 1.尝试获取上一次商品恢复状态
        StockReconcileState state = stockReconcileStateService.getOne(new LambdaQueryWrapper<StockReconcileState>()
                .eq(StockReconcileState::getItemId, itemId)
                .last("LIMIT 1"));
        if (state != null) {
            return state;
        }
        // 2.初始化商品恢复状态
        state = new StockReconcileState();
        state.setItemId(itemId);
        state.setRetryCount(0);
        state.setStatus(STATUS_IGNORED);
        stockReconcileStateService.save(state);
        return state;
    }

    /**
     * 清除当前商品的恢复状态
     * @param itemId 当前商品id
     */
    private void clearStateIfPresent(Long itemId) {
        stockReconcileStateService.remove(new LambdaQueryWrapper<StockReconcileState>()
                .eq(StockReconcileState::getItemId, itemId));
    }

    /**
     * 获取redis对应商品的库存数量和版本快照
     * @param stockKey 库存key，用于取出商品库存数量——用于redis与mysql库存对照
     * @param versionKey 版本key
     * @return 返回redis快照RedisStockSnapshot
     */
    private RedisStockSnapshot readRedisSnapshot(String stockKey, String versionKey) {
        RedisStockSnapshot snapshot = new RedisStockSnapshot();
        snapshot.redisStockStr = stringRedisTemplate.opsForValue().get(stockKey);
        snapshot.redisVersion = stringRedisTemplate.opsForValue().get(versionKey);
        // 获取库存数量：string->Integer
        try {
            snapshot.redisStock = snapshot.redisStockStr == null ? null : Integer.valueOf(snapshot.redisStockStr);
        } catch (Exception ignore) {
            snapshot.redisStock = null;
        }
        // 获取版本快照
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
        // redis库存和版本健壮性判断
        if (redisSnapshot.redisStock == null) {
            return "REDIS_STOCK_MISSING_OR_INVALID";
        }
        if (redisSnapshot.redisEpoch == null || redisSnapshot.redisSeq == null) {
            return "REDIS_VERSION_MISSING_OR_INVALID";
        }
        // 因果律判断：redis在mysql前执行，库存数量不可能大于mysql，如果大于则说明发生极端宕机情况
        if (redisSnapshot.redisStock > item.getStock()) {
            return "REDIS_STOCK_GT_MYSQL";
        }
        // 纪元号判断：若redis的纪元号大于MySQL，则说明redis发生过宕机
        if (redisSnapshot.redisEpoch > mysqlVersion.getMysqlEpoch()
                && redisSnapshot.redisSeq < mysqlVersion.getMysqlSeq()) {
            return "REDIS_SEQ_ROLLBACK_AFTER_EPOCH_CHANGE";
        }
        // 库存判断
        if (!item.getStock().equals(redisSnapshot.redisStock)) {
            return "STOCK_NOT_EQUAL_OBSERVE";
        }
        return null;
    }

    private boolean isUnsafeDiff(String diffType) {
        return "REDIS_STOCK_GT_MYSQL".equals(diffType)
                || "REDIS_SEQ_ROLLBACK_AFTER_EPOCH_CHANGE".equals(diffType);
    }

    /**
     * 应立即忽略
     * @param diffType 差异类型
     * @return 返回false
     */
    private boolean shouldIgnoreImmediately(String diffType) {
        return false;
    }

    /**
     * 人工介入：如果重试次数超过5次并且（redis库存不存在或者redis版本不存在）
     * @param diffType 版本差异类型
     * @param state 库存恢复状态：0-观察中,1-已修复,2-忽略,3-人工介入
     * @return 返回是否需要人工介入boolean
     */
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
        // 进行指数退避
        int backoffSeconds = Math.min((1 << Math.min(retryCount, 8)), MAX_BACKOFF_SECONDS);
        LocalDateTime nextRetryAt = LocalDateTime.now().plusSeconds(backoffSeconds);
        // 填充状态并更新mysql
        fillState(state, item, mysqlVersion, redisSnapshot, diffType, retryCount, nextRetryAt, STATUS_OBSERVING);
        stockReconcileStateService.updateById(state);

        //若指数退避的重试次数超过三次，则返回修复状态1。否则返回观察状态0
        if (retryCount >= 3) {
            return new BackoffDecision(STATUS_REPAIRED);
        }
        return new BackoffDecision(STATUS_OBSERVING);
    }

    /**
     * 更新版本对照表的商品状态
     * @param item 商品ID
     * @param mysqlVersion 商品mysql版本
     * @param redisSnapshot 商品redis快照
     * @param state 商品状态
     * @param diffType 商品差异类型
     */
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

    /**
     * 将商品状态改为需要人工处理
     * @param item 商品ID
     * @param mysqlVersion 商品mysql版本
     * @param redisSnapshot 商品redis快照
     * @param state 商品状态
     * @param diffType 商品差异类型
     */
    private void markManual(Item item, ItemStockVersion mysqlVersion,
                            RedisStockSnapshot redisSnapshot, StockReconcileState state, String diffType) {
        fillState(state, item, mysqlVersion, redisSnapshot, diffType,
                state.getRetryCount() == null ? 0 : state.getRetryCount(), null, STATUS_MANUAL);
        stockReconcileStateService.updateById(state);
    }

    /**
     * 将商品状态恢复用到的redis版本key和mysql版本存到MAP集合
     * @param repairMap 用于恢复商品状态的map集合
     * @param stockKey redis库存key
     * @param versionKey redis版本key
     * @param item 商品
     * @param mysqlVersion mysql版本
     */
    private void putRepairMap(Map<String, String> repairMap, String stockKey, String versionKey,
                              Item item, ItemStockVersion mysqlVersion) {
        repairMap.put(stockKey, String.valueOf(item.getStock()));
        repairMap.put(versionKey, mysqlVersion.getMysqlEpoch()
                + StockVersionConstants.VERSION_SEPARATOR
                + mysqlVersion.getMysqlSeq());
    }

    /**
     * 填充恢复状态字段
     * @param state 状态实体
     * @param item Item实体
     * @param mysqlVersion mysql版本
     * @param redisSnapshot redis快照
     * @param diffType 版本差异类型
     * @param retryCount 重试次数
     * @param nextRetryAt 下一次重试时间
     * @param status 属于哪种int状态：0-观察中,1-已修复,2-忽略,3-人工介入
     */
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

    /**
     * redis的商品版本快照
     */
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
