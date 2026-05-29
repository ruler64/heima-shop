package com.hmall.item.config;

import cn.hutool.core.util.RandomUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmall.item.domain.po.Item;
import com.hmall.item.service.IItemService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
@Order(1) // 先于 EpochInitializer 执行
public class ItemCachePreloader implements ApplicationRunner {

    private final IItemService itemService;
    private final StringRedisTemplate stringRedisTemplate;
    private final RedissonClient redissonClient;

    // 定义缓存 Key
    public static final String ITEM_INDEX_KEY = "item:index:default";
    public static final String ITEM_DETAIL_KEY_PREFIX = "item:detail:";
    // NOTE: Redis Cluster Lua 脚本多 key 运行要求所有 KEYS 落在同一个 hash slot。
    // 因此这里在 key 里使用统一 hash tag：{stock}。
    public static final String ITEM_STOCK_KEY_PREFIX = "item:stock:{stock}:";
    public static final String ITEM_STOCK_VERSION_KEY_PREFIX = "item:stock:ver:{stock}:";
    public static final String ITEM_STOCK_EPOCH_KEY = "item:stock:epoch:{stock}";

    public static final String ITEM_STOCK_HEARTBEAT_KEY = "item:stock:heartbeat:{stock}";
    public static final String ITEM_STOCK_SEQ_KEY_PREFIX = "item:stock:seq:{stock}:";
    private static final String PRELOAD_LOCK_KEY = "item:cache:preload_lock";

    @Override
    public void run(ApplicationArguments args) {
        log.info("[预热引擎] 开始执行热点商品缓存预热");

        // ── 分布式锁：防多节点并发启动时惊群打穿 MySQL ──────────────
        RLock lock = redissonClient.getLock(PRELOAD_LOCK_KEY);
        try {
            // waitTime=35s：等其他节点预热完成再继续，不直接跳过
            // leaseTime=30s：预热最长允许 30s，超时自动释放防死锁
            boolean acquired = lock.tryLock(35, 30, TimeUnit.SECONDS);
            if (!acquired) {
                // 35s 都没等到锁，说明预热节点可能卡住了，记录警告继续启动
                log.warn("[预热引擎] 等待预热锁超时（35s），将继续启动，缓存可能尚未就绪");
                return;
            }

            doPreload();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("[预热引擎] 预热线程被中断", e);
        } catch (Exception e) {
            log.error("[预热引擎] 预热发生未知异常", e);
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    private void doPreload() {
        // ── 读取热点数据 ──────────────────────────────────────────────
        List<Item> hotItems = itemService.list(
                new QueryWrapper<Item>().orderByDesc("update_time").last("LIMIT 500")
        );
        if (hotItems.isEmpty()) {
            log.info("[预热引擎] 无热点商品，跳过预热");
            return;
        }

        // ── Pipeline 批量写入（1500次RTT → 1次RTT）──────────────────
        stringRedisTemplate.executePipelined((RedisCallback<Object>) connection -> {

            for (Item item : hotItems) {
                byte[] detailKey  = serialize(ITEM_DETAIL_KEY_PREFIX + item.getId());
                byte[] detailVal  = serialize(JSONUtil.toJsonStr(item));
                byte[] stockKey   = serialize(ITEM_STOCK_KEY_PREFIX + item.getId());
                byte[] stockVal   = serialize(String.valueOf(item.getStock()));

                // 详情：允许覆盖（MySQL 是详情的事实源），随机 TTL 防雪崩60min+(0~10)min随机
                long detailTTL = 3600L + RandomUtil.randomInt(0, 600);
                connection.stringCommands().setEx(detailKey, detailTTL, detailVal);

                // 库存：无论任何情况一律 setNX
                // 理由：Redis 库存永远领先于 MySQL，用 MySQL 值覆盖只会带入更陈旧的数据
                // key 不存在时（slave丢失）才写入，Failover 丢失的误差由凌晨对账修复
                connection.stringCommands().setNX(stockKey, stockVal);

                // ZSet 索引：不设短 TTL，由定时任务增量维护
                byte[] indexKey = serialize(ITEM_INDEX_KEY);
                long score = item.getUpdateTime() != null
                        ? item.getUpdateTime().toEpochSecond(java.time.ZoneOffset.of("+8")) : 0;
                connection.zSetCommands().zAdd(indexKey, score, serialize(item.getId().toString()));
            }

            // ZSet 设置整体过期时间 24h TTL（配合每日定时全量刷新任务）
            // 不设置永久有效，避免下架商品永久留在榜单
            byte[] indexKey = serialize(ITEM_INDEX_KEY);
            connection.keyCommands().expire(indexKey, 86400L);

            return null; // Pipeline executePipelined 必须返回 null
        });

        log.info("[预热引擎] ✅ Pipeline 预热完成，共加载 {} 条",
                hotItems.size());
    }

    /**
     * 判断是否为 Failover 后重启。
     * 心跳 key 缺失 = 主节点宕机期间无法刷新心跳 → 从节点上心跳已过期。
     */
    private boolean isFailoverRestart() {
        String heartbeat = stringRedisTemplate.opsForValue().get(ITEM_STOCK_HEARTBEAT_KEY);
        String epoch     = stringRedisTemplate.opsForValue().get(ITEM_STOCK_EPOCH_KEY);
        // 心跳消失 或 epoch 消失，均视为 Failover宕机
        return heartbeat == null || epoch == null;
    }

    private byte[] serialize(String value) {
        return stringRedisTemplate.getStringSerializer().serialize(value);
    }
}