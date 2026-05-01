package com.hmall.item.task;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.hmall.item.constants.StockVersionConstants;
import com.hmall.item.domain.po.Item;
import com.hmall.item.service.IItemService;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 方案 B：库存对账与收敛任务。
 * <p>
 * 职责：比对 MySQL 与 Redis 的库存、epoch、seq、version。
 * <p>
 * 规则：
 * 1) 先判断 Redis 是否发生过世代切换/版本缺失；
 * 2) 若存在差异，不立即强覆盖；
 * 3) 使用指数退避记录待收敛状态；
 * 4) 在连续观察到稳定且退避窗口到期后，才允许用 MySQL 事实源回填 Redis。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StockReconciliationJobHandler {

    private static final int MAX_BACKOFF_SECONDS = 300;

    private final IItemService itemService;
    private final StringRedisTemplate stringRedisTemplate;

    @XxlJob("stockReconciliationJob")
    public void execute() {
        XxlJobHelper.log("方案B库存对账任务启动...");

        int pageSize = 500;
        long lastId = 0L;
        int diffCount = 0;
        int repairedCount = 0;

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
                String stockKey = StockVersionConstants.STOCK_KEY_PREFIX + item.getId();
                String versionKey = StockVersionConstants.STOCK_VERSION_KEY_PREFIX + item.getId();
                String reconcileKey = StockVersionConstants.STOCK_RECONCILE_KEY_PREFIX + item.getId();

                String redisStockStr = stringRedisTemplate.opsForValue().get(stockKey);
                String redisVersion = stringRedisTemplate.opsForValue().get(versionKey);
                String redisEpochStr = null;
                String redisSeqStr = null;
                if (redisVersion != null && redisVersion.contains(StockVersionConstants.VERSION_SEPARATOR)) {
                    String[] parts = redisVersion.split("\\|", 2);
                    if (parts.length == 2) {
                        redisEpochStr = parts[0];
                        redisSeqStr = parts[1];
                    }
                }

                Integer redisStock = null;
                try {
                    redisStock = redisStockStr == null ? null : Integer.valueOf(redisStockStr);
                } catch (Exception ignore) {
                    redisStock = null;
                }

                boolean stockMismatch = redisStock == null || !item.getStock().equals(redisStock);
                boolean versionMissing = redisVersion == null || redisEpochStr == null || redisSeqStr == null;

                if (!stockMismatch && !versionMissing) {
                    // Redis 与 MySQL 同步良好，清理可能残留的对账标记
                    stringRedisTemplate.delete(reconcileKey);
                    continue;
                }

                diffCount++;
                XxlJobHelper.log("库存差异或版本缺失，itemId={}, mysql={}, redis={}, redisVer={}",
                        item.getId(), item.getStock(), redisStockStr, redisVersion);

                Map<Object, Object> reconcileState = stringRedisTemplate.opsForHash().entries(reconcileKey);
                int retryCount = 0;
                long nextRetryAt = 0L;
                if (reconcileState != null && !reconcileState.isEmpty()) {
                    Object retryObj = reconcileState.get("retryCount");
                    Object nextObj = reconcileState.get("nextRetryAt");
                    if (retryObj != null) {
                        try {
                            retryCount = Integer.parseInt(String.valueOf(retryObj));
                        } catch (Exception ignore) {
                            retryCount = 0;
                        }
                    }
                    if (nextObj != null) {
                        try {
                            nextRetryAt = Long.parseLong(String.valueOf(nextObj));
                        } catch (Exception ignore) {
                            nextRetryAt = 0L;
                        }
                    }
                }

                long now = System.currentTimeMillis();
                if (now < nextRetryAt) {
                    continue;
                }

                retryCount++;
                int backoffSeconds = Math.min((1 << Math.min(retryCount, 8)), MAX_BACKOFF_SECONDS);
                long newNextRetryAt = now + Duration.ofSeconds(backoffSeconds).toMillis();

                // 先记录待收敛状态，不直接强覆盖 Redis
                stringRedisTemplate.opsForHash().put(reconcileKey, "retryCount", String.valueOf(retryCount));
                stringRedisTemplate.opsForHash().put(reconcileKey, "nextRetryAt", String.valueOf(newNextRetryAt));
                stringRedisTemplate.opsForHash().put(reconcileKey, "mysqlStock", String.valueOf(item.getStock()));
                stringRedisTemplate.opsForHash().put(reconcileKey, "redisStock", String.valueOf(redisStockStr));
                stringRedisTemplate.opsForHash().put(reconcileKey, "redisVersion", String.valueOf(redisVersion));
                stringRedisTemplate.opsForHash().put(reconcileKey, "lastCheckTime", String.valueOf(LocalDateTime.now()));

                // 连续多轮稳定差异后，才允许回填；避免 MQ 飞行消息未落地时误覆盖
                if (retryCount >= 3 && stockMismatch && versionMissing) {
                    repairMap.put(stockKey, String.valueOf(item.getStock()));
                    repairMap.put(versionKey, "1|0");
                    repairedCount++;
                    XxlJobHelper.log("满足收敛条件，回填 Redis 库存，itemId={}, mysqlStock={}", item.getId(), item.getStock());
                    stringRedisTemplate.delete(reconcileKey);
                }
            }

            if (!repairMap.isEmpty()) {
                stringRedisTemplate.opsForValue().multiSet(repairMap);
                XxlJobHelper.log("本批次已修复 Redis 库存 {} 项", repairMap.size() / 2);
            }
        }

        XxlJobHelper.log("方案B库存对账任务完成，累计差异商品数={}，已收敛修复={} 项", diffCount, repairedCount);
    }
}
