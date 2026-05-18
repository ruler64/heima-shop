package com.hmall.item.config;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.hmall.item.config.ItemCachePreloader;
import com.hmall.item.domain.po.Item;
import com.hmall.item.service.IItemService;
import com.hmall.item.service.IItemStockVersionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Redis Cluster failover 后的 epoch 自动恢复组件。
 *
 * <p>触发时机：服务启动时发现 epoch key 不存在（Redis 宕机后 key 丢失）。
 *
 * <p>恢复策略：
 * 1. 从 MySQL item_stock_version 查 MAX(mysql_epoch) 作为上一个安全纪元；
 * 2. newEpoch = max + 1，写入全局 epoch key（setIfAbsent 防多实例并发）；
 * 3. 重置全局 seq = 0；
 * 4. 批量将所有商品的 per-item version key 写为 newEpoch|0；
 *    —— 使对账时 redisEpoch > mysqlEpoch，触发 REDIS_SEQ_ROLLBACK_AFTER_EPOCH_CHANGE，
 *       凌晨对账任务将自动修复 Redis 库存。
 */
@Slf4j
@Component
@RequiredArgsConstructor
@Order(2) // 必须在 ItemCachePreloader(Order=1) 之后执行
public class EpochInitializer implements ApplicationRunner {

    private final StringRedisTemplate redisTemplate;
    private final IItemStockVersionService stockVersionService;
    private final IItemService itemService;

    @Override
    public void run(ApplicationArguments args) {
        String redisEpoch = redisTemplate.opsForValue().get(ItemCachePreloader.ITEM_STOCK_EPOCH_KEY);

        if (redisEpoch != null) {
            log.info("[EpochInitializer] Redis epoch 存在，值={}，无 failover，跳过", redisEpoch);
            return;
        }

        log.warn("[EpochInitializer] 检测到 epoch key 缺失，判定为 Redis Cluster failover，开始 epoch 升级");
        handleEpochLoss();
    }

    // -------------------------------------------------------------------------

    private void handleEpochLoss() {
        // Step 1：从 MySQL 获取上一个安全 epoch
        Long maxMysqlEpoch = stockVersionService.getMaxMysqlEpoch();
        long newEpoch = (maxMysqlEpoch == null ? 0L : maxMysqlEpoch) + 1;

        // Step 2：原子写入，防止多实例并发启动时重复升级
        Boolean acquired = redisTemplate.opsForValue()
                .setIfAbsent(ItemCachePreloader.ITEM_STOCK_EPOCH_KEY, String.valueOf(newEpoch));

        if (!Boolean.TRUE.equals(acquired)) {
            String currentEpoch = redisTemplate.opsForValue().get(ItemCachePreloader.ITEM_STOCK_EPOCH_KEY);
            log.info("[EpochInitializer] epoch 已由其他实例升级，当前值={}，本实例跳过", currentEpoch);
            return;
        }

        // Step 3：重置全局 seq（新纪元从 0 起计）
        redisTemplate.opsForValue().set(ItemCachePreloader.ITEM_STOCK_SEQ_KEY, "0");

        log.warn("[EpochInitializer] epoch 升级完成: maxMysqlEpoch={} → newRedisEpoch={}", maxMysqlEpoch, newEpoch);

        // Step 4：批量刷新所有商品的 per-item version key 为 newEpoch|0
        //         目的：让对账时 redisEpoch(newEpoch) > mysqlEpoch(maxMysqlEpoch)
        //         触发 REDIS_SEQ_ROLLBACK_AFTER_EPOCH_CHANGE，凌晨对账自动修复库存
        flushItemVersionKeys(newEpoch);
    }

    /**
     * Pipeline 批量写入每商品版本 key，避免大量单次 RTT。
     * 商品量大时可改为分页游标写入。每商品版本 key= item:stock:ver:{stock}:itemId,对应value：epoch|seq
     */
    private void flushItemVersionKeys(long newEpoch) {
        // 与预热保持一致的范围：取 update_time 最近的 500 条活跃商品
        List<Long> itemIds = itemService.list(
                new LambdaQueryWrapper<Item>()
                        .eq(Item::getStatus, 1)
                        .orderByDesc(Item::getUpdateTime)
                        .last("LIMIT 500")
        ).stream().map(Item::getId).collect(Collectors.toList());

        if (itemIds.isEmpty()) {
            log.warn("[EpochInitializer] 无活跃商品，跳过 per-item version key 刷新");
            return;
        }

        String newVersion = newEpoch + "|0";
        byte[] versionBytes = newVersion.getBytes(StandardCharsets.UTF_8);

        // Redis Pipeline：所有写操作一次网络往返
        redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (Long itemId : itemIds) {
                String versionKey = ItemCachePreloader.ITEM_STOCK_VERSION_KEY_PREFIX + itemId;
                connection.set(
                        versionKey.getBytes(StandardCharsets.UTF_8),
                        versionBytes
                );
            }
            return null;
        });

        log.warn("[EpochInitializer] 已将 {} 个商品的 per-item version key 升级为 [{}]",
                itemIds.size(), newVersion);
    }
}