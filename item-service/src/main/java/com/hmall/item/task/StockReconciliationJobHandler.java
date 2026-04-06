package com.hmall.item.task;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmall.item.config.ItemCachePreloader;
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
import java.util.stream.Collectors;

/**
 * 全量库存离线对账与补偿任务
 * 建议调度时间：每天凌晨 03:00:00 (Cron: 0 0 3 * * ?)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StockReconciliationJobHandler {

    private final IItemService itemService;
    private final StringRedisTemplate stringRedisTemplate;

    // Redis 中库存的 Key 前缀
    private static final String ITEM_STOCK_KEY_PREFIX = ItemCachePreloader.ITEM_DETAIL_KEY_PREFIX;

    @XxlJob("stockReconciliationJob")
    public void execute() {
        XxlJobHelper.log("开始执行凌晨库存全量对账任务...");
        long startTime = System.currentTimeMillis();

        int pageSize = 1000; // 每次处理 1000 条，防 OOM
        int pageNo = 1;
        int diffCount = 0;   // 记录出现差异的商品数量

        while (true) {
            // 1. 分批从 MySQL 中查出已上架的商品库存
            Page<Item> page = itemService.lambdaQuery()
                    .eq(Item::getStatus, 1) // 1 表示上架状态
                    .select(Item::getId, Item::getStock) // 性能优化：只查 ID 和 Stock
                    .page(new Page<>(pageNo, pageSize));

            List<Item> records = page.getRecords();
            if (records == null || records.isEmpty()) {
                break; // 处理完毕
            }

            // 2. 批量构建 Redis 的 Keys
            List<String> redisKeys = records.stream()
                    .map(item -> ITEM_STOCK_KEY_PREFIX + item.getId())
                    .collect(Collectors.toList());

            // 3. 【大厂细节】使用 MultiGet 批量获取 Redis 中的库存，极大减少网络 I/O
            List<String> redisStocks = stringRedisTemplate.opsForValue().multiGet(redisKeys);

            // 用于存放需要修复的 Redis 数据
            Map<String, String> repairMap = new HashMap<>();

            // 4. 开始对账比对
            for (int i = 0; i < records.size(); i++) {
                Item mysqlItem = records.get(i);
                String redisStockStr = redisStocks != null ? redisStocks.get(i) : null;
                Integer redisStock = redisStockStr != null ? Integer.valueOf(redisStockStr) : null;

                // 判断是否存在差异 (包含 Redis 中完全没有该商品库存的情况)
                if (redisStock == null || !mysqlItem.getStock().equals(redisStock)) {
                    diffCount++;
                    String logMsg = String.format("发现库存差异！商品ID: %d, MySQL真实库存: %d, Redis虚假库存: %s",
                            mysqlItem.getId(), mysqlItem.getStock(), redisStockStr == null ? "NULL" : redisStockStr);

                    log.warn(logMsg);
                    XxlJobHelper.log(logMsg); // 同步输出到 XXL-Job 调度中心日志

                    // 加入待修复集合
                    repairMap.put(ITEM_STOCK_KEY_PREFIX + mysqlItem.getId(), String.valueOf(mysqlItem.getStock()));
                }
            }

            // 5. 【大厂细节】如果发现差异，使用 MultiSet 批量覆写修复 Redis
            if (!repairMap.isEmpty()) {
                stringRedisTemplate.opsForValue().multiSet(repairMap);
                log.info("已完成本批次 {} 个商品的 Redis 库存强行覆盖修复", repairMap.size());
            }

            // 翻页
            pageNo++;
        }

        long costTime = System.currentTimeMillis() - startTime;
        String finishMsg = String.format("库存全量对账任务执行完毕！耗时: %d ms, 累计修复异常商品数: %d 个", costTime, diffCount);
        log.info(finishMsg);
        XxlJobHelper.log(finishMsg);

        // 如果差异数量过大，说明白天的 MQ 可能出现了大面积故障，可以在这里触发钉钉/企业微信严重告警
        if (diffCount > 100) {
            log.error("【严重警告】昨夜发生的库存差异数量高达 {}，请尽快排查 MQ 或业务日志！", diffCount);
            // TODO: 调用告警系统 API
        }
    }
}