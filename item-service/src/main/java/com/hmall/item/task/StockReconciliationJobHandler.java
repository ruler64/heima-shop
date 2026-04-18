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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 方案 B：库存对账任务
 * <p>
 * 职责：按商品逐批比对 MySQL 与 Redis 库存，并修正 Redis 侧异常值。
 * <p>
 * 规则：MySQL 作为最终事实源；Redis 的库存值和版本号如果异常，按 MySQL 覆盖修复。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StockReconciliationJobHandler {

    private final IItemService itemService;
    private final StringRedisTemplate stringRedisTemplate;

    @XxlJob("stockReconciliationJob")
    public void execute() {
        XxlJobHelper.log("方案B库存对账任务启动...");

        int pageSize = 500;
        long lastId = 0L;
        int diffCount = 0;

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

                String redisStockStr = stringRedisTemplate.opsForValue().get(stockKey);
                String redisVersion = stringRedisTemplate.opsForValue().get(versionKey);

                Integer redisStock = redisStockStr == null ? null : Integer.valueOf(redisStockStr);
                if (redisStock == null || !item.getStock().equals(redisStock)) {
                    diffCount++;
                    XxlJobHelper.log("库存差异，itemId={}, mysql={}, redis={}, redisVer={}",
                            item.getId(), item.getStock(), redisStockStr, redisVersion);
                    repairMap.put(stockKey, String.valueOf(item.getStock()));
                    // 对账修复时，版本统一归零，后续业务变更再按 Lua 脚本递增
                    repairMap.put(versionKey, "0|0");
                }
            }

            if (!repairMap.isEmpty()) {
                stringRedisTemplate.opsForValue().multiSet(repairMap);
                XxlJobHelper.log("本批次已修复 Redis 库存 {} 项", repairMap.size() / 2);
            }
        }

        XxlJobHelper.log("方案B库存对账任务完成，累计差异商品数={}", diffCount);
    }
}
