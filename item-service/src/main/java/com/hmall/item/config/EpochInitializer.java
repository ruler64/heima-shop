package com.hmall.item.config;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.hmall.item.config.ItemCachePreloader;
import com.hmall.item.constants.RedisConstants;
import com.hmall.item.domain.po.Item;
import com.hmall.item.service.IItemService;
import com.hmall.item.service.IItemStockVersionService;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Redis Cluster failover 后的 epoch 自动恢复组件。
 *
 * <p>触发时机：服务启动时发现 epoch key 不存在（Redis 宕机后 key 丢失）。
 *
 * <p>恢复策略：
 * <ol>
 *   <li> 从 MySQL item_stock_version 查 MAX(mysql_epoch) 作为上一个安全纪元；</li>
 *   <li> newEpoch = max + 1，写入全局 epoch key（setIfAbsent 防多实例并发）；</li>
 *   <li>重置全局 seq = 0；</li>
 *   <li>批量将所有商品的 per-item version key 写为 newEpoch|0；
 *      —— 使对账时 redisEpoch > mysqlEpoch，触发 REDIS_SEQ_ROLLBACK_AFTER_EPOCH_CHANGE，
 *         凌晨对账任务将自动修复 Redis 库存。</li>
 * </ol>
 */
@Slf4j
@Component
@RequiredArgsConstructor
@Order(2) // 必须在 ItemCachePreloader(Order=1) 之后执行
public class EpochInitializer implements ApplicationRunner {

    private final StringRedisTemplate redisTemplate;
    private final IItemStockVersionService stockVersionService;
    private final IItemService itemService;
    private final RedisFailoverDetector failoverDetector;  // 复用 Failover 探测逻辑
    private final MeterRegistry meterRegistry; // 🌟 注入 Micrometer 监控注册表
    @Autowired
    @Qualifier("batchPreloadStockScript")
    private DefaultRedisScript<Long> batchPreloadStockScript;

    @Override
    public void run(ApplicationArguments args) {
        log.info("执行[EpochInitializer]............");
        String epochVal   = redisTemplate.opsForValue().get(RedisConstants.LUA_EPOCH);
        String heartbeat  = redisTemplate.opsForValue().get(RedisConstants.REDIS_HEARTBEAT_KEY);

        if (epochVal == null) {
            // 探针1：epoch key 完全消失 → 全量数据丢失
            log.warn("[EpochInitializer] epoch key 不存在，判定为 Redis 全量宕机或首次部署，开始 epoch 初始化");
            doHandleFailover("EPOCH_KEY_MISSING");

        } else if (heartbeat == null) {
            // 探针2：epoch 存在但心跳消失 → 从节点晋升，可能丢失部分写入
            // 保守策略：epoch+1，触发凌晨对账全量修复库存
            log.warn("[EpochInitializer] heartbeat key 不存在但 epoch={} 存在，" +
                    "保守判定为从节点晋升（主从延迟写丢失），执行 epoch 升级", epochVal);
            doHandleFailover("HEARTBEAT_MISSING");

        } else {
            // 正常重启：两个探针都存在，无需处理
            log.info("[EpochInitializer] Redis 状态正常，epoch={}，心跳存在，跳过 epoch 升级", epochVal);
        }
        /*if (epochVal != null) {
            log.info("[EpochInitializer] Redis epoch 存在，值={}，无 failover，跳过", epochVal);
            return;
        }

        log.warn("[EpochInitializer] 检测到 epoch key 缺失，判定为 Redis Cluster failover，开始 epoch 升级");
        handleEpochLoss();*/
    }

    // -------------------------------------------------------------------------

    /**
     * 委托给 RedisFailoverDetector 执行原子 epoch 升级，
     * 避免两套平行逻辑出现漂移。
     */
    private void doHandleFailover(String reason) {
        try {
            boolean upgraded = failoverDetector.executeEpochUpgrade();
            if (upgraded) {
                log.warn("[EpochInitializer] epoch 升级成功，原因={}，开始刷新所有活跃商品版本 key", reason);
                meterRegistry.counter("redis_epoch_init_total", "reason", reason).increment();
                // 升级 epoch 后，批量将所有活跃商品 ver key 写为 newEpoch| pre_sequence
                // 目的：让凌晨对账感知到 epoch 变化，触发全量库存修复
                String newEpoch = redisTemplate.opsForValue().get(RedisConstants.LUA_EPOCH);
                if (newEpoch != null) {
                    flushAllItemVersionKeys(Long.parseLong(newEpoch));
                }
            } else {
                log.info("[EpochInitializer] 其他实例已完成 epoch 升级，本实例跳过，原因={}", reason);
            }
        } catch (Exception e) {
            log.error("[EpochInitializer] epoch 升级失败，原因={}，服务将继续启动（运行期看门狗会重试）", reason, e);
            // 不抛异常：允许服务启动，RedisFailoverDetector 的看门狗会持续重试
        }
    }

    /**
     * LUA 批量将所有活跃商品的 per-item version key 写为 {@code newEpoch|pre_sequence}或初始化为{@code newEpoch|0}。
     *
     * <p>目的：让凌晨对账时 redis_epoch > mysql_epoch，
     * 触发 EPOCH_MISMATCH 分支，自动用 MySQL 值修复 Redis 库存。
     *
     * <p>注意：seq key 不重置，让 {@code INCR} 自然从 1 开始，
     * 凌晨对账 repair 后会将 Redis seq 对齐到 MySQL mysql_seq。
     *
     * @param newEpoch 升级后的 epoch 值
     */
    private void flushAllItemVersionKeys(long newEpoch) {
        // 与预热保持一致：取 update_time 最近的 500 条活跃商品
        List<Long> itemIds = itemService.list(
                new LambdaQueryWrapper<Item>()
                        .eq(Item::getStatus, 1)
                        .orderByDesc(Item::getUpdateTime)
                        .last("LIMIT 500")
        ).stream().map(Item::getId).collect(Collectors.toList());

        if (itemIds.isEmpty()) {
            log.warn("[EpochInitializer] 无活跃商品，跳过 per-item version key 批量刷新");
            return;
        }

        // 2. 组装 KEYS (前一半是 ver_key，后一半是 seq_key)
        List<String> keys = new ArrayList<>(itemIds.size() * 2);
        // 先放所有的 ver_key
        for (Long itemId : itemIds) {
            keys.add(ItemCachePreloader.ITEM_STOCK_VERSION_KEY_PREFIX + itemId);
        }
        // 再放所有的 seq_key
        for (Long itemId : itemIds) {
            keys.add(RedisConstants.ITEM_STOCK_SEQ_KEY_PREFIX + itemId);
        }

        String initVersion = newEpoch + "|0";

        // 3. 一次性原子执行（如果 500 个太多怕阻塞，可以拆成 100 个一批）
        redisTemplate.execute(batchPreloadStockScript, keys, initVersion);

        log.warn("[CachePreloader] 安全预热完成，已尝试为 {} 个商品初始化版本 [{}] 与流水号 0",
                itemIds.size(), initVersion);
    }
    /*public void handleEpochLoss() {
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
        //redisTemplate.opsForValue().set(ItemCachePreloader.ITEM_STOCK_SEQ_KEY, "0");

        log.warn("[EpochInitializer] epoch 升级完成: maxMysqlEpoch={} → newRedisEpoch={}", maxMysqlEpoch, newEpoch);
        // 🌟 埋点：服务启动时冷启动初始化计数器 +1
        meterRegistry.counter("redis_epoch_initialization_total", "type", "startup").increment();

        // Step 4：批量刷新所有商品的 per-item version key 为 newEpoch|0
        //         目的：让对账时 redisEpoch(newEpoch) > mysqlEpoch(maxMysqlEpoch)
        //         触发 REDIS_SEQ_ROLLBACK_AFTER_EPOCH_CHANGE，凌晨对账自动修复库存
        log.info("[EpochInitializer] 开始为前 500 个高频核心商品灌入版本标识，激活对账护航...");
        flushItemVersionKeys(newEpoch);
    }

    *//**
     * 安全的缓存预热：使用 Lua 脚本保证 ver 和 seq 的原子性，并且绝对不覆盖已有数据
     *//*
    private void flushItemVersionKeys(long newEpoch) {
        List<Long> itemIds = itemService.list(
                new LambdaQueryWrapper<Item>()
                        .eq(Item::getStatus, 1)
                        .orderByDesc(Item::getUpdateTime)
                        .last("LIMIT 500")
        ).stream().map(Item::getId).collect(Collectors.toList());

        if (itemIds.isEmpty()) {
            return;
        }

        // 2. 组装 KEYS (前一半是 ver_key，后一半是 seq_key)
        List<String> keys = new ArrayList<>(itemIds.size() * 2);
        // 先放所有的 ver_key
        for (Long itemId : itemIds) {
            keys.add(ItemCachePreloader.ITEM_STOCK_VERSION_KEY_PREFIX + itemId);
        }
        // 再放所有的 seq_key
        for (Long itemId : itemIds) {
            keys.add(RedisConstants.ITEM_STOCK_SEQ_KEY_PREFIX + itemId);
        }

        String initVersion = newEpoch + "|0";

        // 3. 一次性原子执行（如果 500 个太多怕阻塞，可以拆成 100 个一批）
        redisTemplate.execute(batchPreloadStockScript, keys, initVersion);

        log.warn("[CachePreloader] 安全预热完成，已尝试为 {} 个商品初始化版本 [{}] 与流水号 0",
                itemIds.size(), initVersion);
    }*/
}