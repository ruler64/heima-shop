package com.hmall.item.task;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.hmall.api.client.TradeClient;
import com.hmall.item.constants.RedisConstants;
import com.hmall.item.domain.po.Item;
import com.hmall.item.domain.po.ItemStockVersion;
import com.hmall.item.domain.po.StockReconcileState;
import com.hmall.item.service.IItemService;
import com.hmall.item.service.IItemStockVersionService;
import com.hmall.item.service.IStockReconcileStateService;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import io.micrometer.core.instrument.MeterRegistry;
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
    private final MeterRegistry meterRegistry;

    @XxlJob("stockReconciliationJob")
    public void execute() {
        XxlJobHelper.log("库存对账任务启动：以 MySQL item + item_stock_version 为事实源...");
        // Step 1: 前置门卫，确认没有扣减消息在飞
        Boolean pendingOutbox = tradeClient.existsPendingOutbox();
        if (Boolean.TRUE.equals(pendingOutbox)) {
            XxlJobHelper.log("检测到 trade-service 本地消息表仍有 status=0 的未完成消息，本轮对账整体退避并告警");
            log.warn("库存对账跳过：trade-service local_event_outbox 存在未完成消息(status=0)");
            return;
        }
        // 核心：任务启动时，先从 Redis 抓取当前最新的全局 Epoch
        long globalEpoch = 1L;
        String epochStr = stringRedisTemplate.opsForValue().get(RedisConstants.LUA_EPOCH);
        if (epochStr != null) {
            try {
                globalEpoch = Long.parseLong(epochStr);
            } catch (Exception e) {
                log.error("解析全局 Epoch 失败", e);
            }
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
                String stockKey = RedisConstants.ITEM_STOCK_KEY_PREFIX + item.getId();
                String versionKey = RedisConstants.STOCK_VERSION_KEY_PREFIX + item.getId();

                // 核心修复点 1：构建独立的 per-item seq key (通过替换 ver 为 seq，完美匹配 Lua 脚本的键结构)
                String seqKey = versionKey.replace("ver", "seq");

                // 获取redis对应商品的库存数量和版本快照
                RedisStockSnapshot redisSnapshot = readRedisSnapshot(stockKey, versionKey);
                // 根据当前mysql版本和redis快照版本的状态->当前属于那个不同的状态
                String diffType = resolveDiffType(item, mysqlVersion, redisSnapshot);
                // 获取或初始化当前的恢复状态
                StockReconcileState state = getOrInitReconcileState(item.getId());

                // 若当前商品的redis与mysql的版本与库存数量对齐了
                if (diffType == null) {
//                    if (state.getStatus() != null && state.getStatus() == STATUS_OBSERVING && state.getRetryCount() != null && state.getRetryCount() > 0) {
//                        // 若之前存在差异进入了观察状态，此时又把差异对齐消除了，那么忽略当前商品
//                        ignoredCount++;
//                        markIgnored(item, mysqlVersion, redisSnapshot, state, "DIFF_AUTO_CONVERGED");
//                    } else {
                        // 如果不存在差异，直接删除对应商品的恢复状态
                        clearStateIfPresent(item.getId());
//                    }
                    continue;
                }
                // 若当前商品存在差异，打印日志
                diffCount++;
                XxlJobHelper.log("库存差异，itemId={}, diffType={}, mysqlStock={}, redisStock={}, mysqlVer={}|{}, redisVer={}|{}",
                        item.getId(), diffType, item.getStock(), redisSnapshot.redisStock,
                        mysqlVersion.getMysqlEpoch(), mysqlVersion.getMysqlSeq(), redisSnapshot.redisEpoch, redisSnapshot.redisSeq);

                // ==================== 核心流转接入告警 ====================

                // 新增判定：如果是 Lua 惰性打标导致的 Epoch 超前，静默同步！
                if ("LAZY_STAMP_PROGRESS".equals(diffType)) {
                    itemStockVersionService.syncEpoch(item.getId(), redisSnapshot.redisEpoch);
                    repairedCount++;
                    continue; // 正常行为，不发告警，直接看下一个商品
                }
                // Step 3: Redis 宕机导致丢了扣减动作：Redis库存 > MySQL库存
                if ("ANOMALY_REDIS_GT_MYSQL".equals(diffType)) {
                    // 钉钉告警
                    reportAnomaly(item.getId(), redisSnapshot.redisStock, item.getStock());
                    // 🌟 核心修复点 2：修复时传入 seqKey
                    doRepair(item, stockKey, versionKey, seqKey, repairMap, redisSnapshot, state, diffType, globalEpoch);
                    repairedCount++;
                    continue;
                }

                // Step 4: Epoch 检查 (Failover 发生)Redis 宕机导致纪元超前且数据错乱（例如 Seq 或 Stock 不一致）
                if ("EPOCH_MISMATCH_WITH_DIFF".equals(diffType)) {
                    // 钉钉告警
                    reportEpochMismatch(item.getId());
                    // 🌟 核心修复点 2：修复时传入 seqKey
                    doRepair(item, stockKey, versionKey, seqKey, repairMap, redisSnapshot, state, diffType, globalEpoch);
                    repairedCount++;
                    continue;
                }

                // 理论不可能状态 (Redis Epoch 小于 MySQL)，直接人工介入
                if ("MYSQL_EPOCH_AHEAD".equals(diffType) || isManualRequired(diffType, state)) {
                    manualCount++;
                    markManual(item, mysqlVersion, redisSnapshot, state, diffType);
                    continue;
                }
                // Redis 宕机导致 seq 小于 MySQL（丢失流水），或者 stock 延迟，进入指数退避
                BackoffDecision decision = observeAndDecide(item, mysqlVersion, redisSnapshot, state, diffType);
                if (decision.status == STATUS_REPAIRED) {
                    // 🌟 核心修复点 2：修复时传入 seqKey
                    doRepair(item, stockKey, versionKey, seqKey, repairMap, redisSnapshot, state, diffType, globalEpoch);
                    repairedCount++;
                    XxlJobHelper.log("连续观察后满足收敛条件，按 MySQL 事实源回填 Redis，itemId={}", item.getId());
                } else if (decision.status == STATUS_MANUAL) {
                    manualCount++;
                    markManual(item, mysqlVersion, redisSnapshot, state, diffType);
                }
            }

            if (!repairMap.isEmpty()) {
                //在底层会直接被翻译成 Redis 的原生原子命令：MSET，可替代复杂lua脚本，但不允许crossSlot
                stringRedisTemplate.opsForValue().multiSet(repairMap);
                XxlJobHelper.log("本批次已修复 Redis 库存 {} 项", repairMap.size() / 3);// 由于每个商品多写了一个 seqKey，这里除以 3
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
        if (snapshot.redisVersion != null && snapshot.redisVersion.contains(RedisConstants.VERSION_SEPARATOR)) {
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
        if (redisSnapshot.redisStock == null || redisSnapshot.redisEpoch == null || redisSnapshot.redisSeq == null) {
            return "MISSING_DATA";
        }
        // 因果律判断：redis在mysql前执行，库存数量不可能大于mysql，如果大于则说明绝对是丢了扣减！严重异常！
        if (redisSnapshot.redisStock > item.getStock()) {
            return "ANOMALY_REDIS_GT_MYSQL";
        }
        // 纪元号判断：若redis的纪元号大于MySQL，则说明redis发生过宕机
        if (redisSnapshot.redisEpoch > mysqlVersion.getMysqlEpoch()) {
            // 2. 惰性打标检测：纪元超前，但是流水号和库存完全一致，这说明没有丢数据，只是 Lua 更新了纪元
            if (redisSnapshot.redisSeq.equals(mysqlVersion.getMysqlSeq()) && redisSnapshot.redisStock.equals(item.getStock())) {
                return "LAZY_STAMP_PROGRESS";
            }
            // 否则，是真的发生了纪元和数据的双重错位
            return "EPOCH_MISMATCH_WITH_DIFF";
        }
        // 几乎不可能
        if (redisSnapshot.redisEpoch < mysqlVersion.getMysqlEpoch()) {
            return "MYSQL_EPOCH_AHEAD";
        }
        // 每个item的流水号判断，只可能redis>=mysql否则发生过宕机
        if (!redisSnapshot.redisSeq.equals(mysqlVersion.getMysqlSeq())) {
            return "SEQ_MISMATCH";
        }
        // 库存判断，若之前都正常，库存几乎不可能不相等，除非手动在MySQL添加了
        if (!item.getStock().equals(redisSnapshot.redisStock)) {
            return "STOCK_MISMATCH_SAME_VERSION";
        }
        return null;
    }

    /**
     * 🌟 核心修复点 3：增加 seqKey 参数
     */
    private void doRepair(Item item, String stockKey, String versionKey, String seqKey, Map<String, String> repairMap,
                          RedisStockSnapshot redisSnapshot, StockReconcileState state, String diffType, long globalEpoch) {
        ItemStockVersion currentVersion = getOrInitMysqlVersion(item.getId());
        // 计算应该使用的最新 Epoch（不能比 MySQL 本地旧，且必须跟上全局步伐）
        long targetEpoch = Math.max(currentVersion.getMysqlEpoch() + 1, globalEpoch);
        // 更新 MySQL (只改纪元，保留 mysql_seq)
        itemStockVersionService.recordReconcileRepair(item.getId(),targetEpoch);
        // 获取更新后的数据源
        ItemStockVersion repairedVersion = getOrInitMysqlVersion(item.getId());
        // 覆盖写入 Redis Map
        putRepairMap(repairMap, stockKey, versionKey, seqKey, item, repairedVersion); // 存入本批次 pipeline
        markRepaired(item, repairedVersion, redisSnapshot, state, diffType); // 更新修复状态表
    }

    /**
     * 人工介入：如果重试次数超过5次并且（redis库存不存在或者redis版本不存在）
     * @param diffType 版本差异类型
     * @param state 库存恢复状态：0-观察中,1-已修复,2-忽略,3-人工介入
     * @return 返回是否需要人工介入boolean
     */
    private boolean isManualRequired(String diffType, StockReconcileState state) {
        return state.getRetryCount() != null && state.getRetryCount() >= 5
                && ("MISSING_DATA".equals(diffType) || "STOCK_MISMATCH_SAME_VERSION".equals(diffType));
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
    private void putRepairMap(Map<String, String> repairMap, String stockKey, String versionKey, String seqKey,
                              Item item, ItemStockVersion mysqlVersion) {
        // 1. 修正库存 Key (对应 Lua 中的 KEYS[1..n])
        // 例如：key = item:stock:{stock}:1001, value = 99 (来自 MySQL 真实的 stock)
        repairMap.put(stockKey, String.valueOf(item.getStock()));
        // 2. 修正 Version Key (对应 Lua 中的 KEYS[2n+4..3n+3])
        // 例如：key = item:stock:ver:{stock}:1001, value = "2|5" (最新的大盘 Epoch | MySQL 的 Seq)
        repairMap.put(versionKey, mysqlVersion.getMysqlEpoch()
                + RedisConstants.VERSION_SEPARATOR
                + mysqlVersion.getMysqlSeq());
        // 3. 修正 Seq Key (对应 Lua 中的 KEYS[n+4..2n+3])
        // 例如：key = item:stock:seq:{stock}:1001, value = 5 (MySQL 的 Seq)
        repairMap.put(seqKey, String.valueOf(mysqlVersion.getMysqlSeq()));
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
    // ANOMALY 发生时
    private void reportAnomaly(Long itemId, long redisStock, long mysqlStock) {
        log.error("[对账] ANOMALY: Redis库存({})>MySQL库存({})，商品itemId={}",
                redisStock, mysqlStock, itemId);
        meterRegistry.counter("stock_reconciliation_anomaly_total",
                "itemId", String.valueOf(itemId),
                "type", "REDIS_GREATER_THAN_MYSQL"
        ).increment();
    }

    // EPOCH_MISMATCH repair 发生时
    private void reportEpochMismatch(Long itemId) {
        log.warn("[对账] ⚠️ EPOCH_MISMATCH触发: 发现宕机重切后遗症，商品itemId={}", itemId);
        meterRegistry.counter("stock_reconciliation_repair_total",
                "itemId", String.valueOf(itemId),
                "reason", "EPOCH_MISMATCH"
        ).increment();
    }
}
