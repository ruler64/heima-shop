package com.hmall.item.config;

import com.hmall.item.constants.RedisConstants;
import com.hmall.item.service.IItemStockVersionService;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RedissonClient;
import org.redisson.connection.ConnectionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Redis Cluster Failover 运行期持续监控与自愈组件。
 *
 * <p><b>整体架构：脏标志位 + 心跳双探针 + 看门狗</b>
 *
 * <pre>
 * ┌─────────────────────────────────────────────────────────┐
 * │  Layer1  ConnectionListener（Redisson 连接事件）          │
 * │  作用：极轻量感知连接变化，仅翻转 topologyDirtyFlag          │
 * │  注意：不做任何 Redis 调用，防止回调风暴                     │
 * └───────────────────────────┬─────────────────────────────┘
 *                             │ (每 5s)
 * ┌───────────────────────────▼─────────────────────────────┐
 * │  Layer2  cronWatchdog（定时看门狗）                       │
 * │  三探针决策：                                             │
 * │    探针1  epoch key 消失      → 全量数据丢失               │
 * │    探针2  heartbeat key 消失  → 从节点晋升                 │
 * │    探针3  topologyDirtyFlag   → 连接层感知到拓扑变化        │
 * │  任一触发 → 调用 executeEpochUpgrade()                    │
 * └───────────────────────────┬─────────────────────────────┘
 *                             │
 * ┌───────────────────────────▼─────────────────────────────┐
 * │  Layer3  executeEpochUpgrade()（Lua 原子升级）            │
 * │  核心防重机制：                                           │
 * │    Step1  doneKey 存在？→ 0（已处理，直接返回）             │
 * │    Step2a epochKey 存在 → INCR → 写 doneKey              │
 * │    Step2b epochKey 不存在 → SET initEpoch → 写 doneKey   │
 * └─────────────────────────────────────────────────────────┘
 * </pre>
 *
 * <p><b>为什么连接监听器不会误触发</b>：
 * 连接池新建/复用连接时确实会触发 {@code onConnect}，
 * 但只是一次无锁 CAS 操作（{@code AtomicBoolean.set(true)}），
 * 真正的判断和 Redis 调用发生在看门狗（每5s一次），
 * 看门狗内部通过心跳探针和 epoch 探针双重确认，不会误升级。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RedisFailoverDetector {

    // ─────────────────────────────────────────────────────────
    // 常量配置
    // ─────────────────────────────────────────────────────────

    /** 分布式锁超时时间（秒）。足够覆盖一次 epoch 升级的执行时间。 */
    private static final int LOCK_TTL_SECONDS = 45;

    /** 看门狗轮询间隔（毫秒）。fixedDelay 确保上次执行完毕后再等待。 */
    private static final long WATCHDOG_INTERVAL_MS = 5_000L;

    // ─────────────────────────────────────────────────────────
    // 状态标志位（原子操作，无锁）
    // ─────────────────────────────────────────────────────────

    /**
     * 拓扑脏标志位。
     * 由 Redisson ConnectionListener 在连接事件时翻转为 true，
     * 由看门狗在成功处理后重置为 false。
     * 不论网络抖动触发多少次回调，只是 AtomicBoolean 的 CAS 操作，无性能损耗。
     */
    private final AtomicBoolean topologyDirtyFlag = new AtomicBoolean(false);

    // 注入 RedissonClient 拿到真实的底层连接事件
    private final RedissonClient redissonClient;

    /**
     * 处理中守卫。
     * 防止上一次看门狗还未执行完时，下一次定时触发重叠执行。
     */
    private final AtomicBoolean isProcessing = new AtomicBoolean(false);

    // ─────────────────────────────────────────────────────────
    // 依赖注入
    // ─────────────────────────────────────────────────────────

    private final StringRedisTemplate redisTemplate;
    private final IItemStockVersionService stockVersionService;
    private final MeterRegistry meterRegistry;
    private final RedisHeartbeatRefresher heartbeatRefresher;

    /** Failover epoch 升级 Lua 脚本（预加载，避免运行期重复编译）。 */
    @Autowired
    @Qualifier("failoverEpochUpgrade")
    private DefaultRedisScript<Long> FAILOVER_LUA_SCRIPT;

    // ─────────────────────────────────────────────────────────
    // Redisson 连接监听器注册（应用启动后由 EpochInitializer 调用）
    // ─────────────────────────────────────────────────────────

    /**
     * 标记拓扑脏状态，供 Redisson ConnectionListener 回调调用。
     * 方法体只有一条 CAS 指令，绝对不在此处发起任何 Redis 调用。
     */
    public void markTopologyDirty() {
        topologyDirtyFlag.set(true);
    }
    /**
     * 【真正补齐 Layer1 毫秒级探针】
     * 在 Bean 初始化时，向 Redisson 注册集群节点连接监听器
     */
    /*@PostConstruct
    public void initReactiveProbe() {
        redissonClient.getClusterNodesGroup().addConnectionListener(new ConnectionListener() {
            @Override
            public void onConnect(InetSocketAddress address) {
                // 毫秒级触发：一旦发现与某个节点重新建连或槽位变更
                log.warn("[RedisFailover] 📡 监听到 Redis 集群拓扑重连事件，目标节点: {}", address);
                topologyDirtyFlag.set(true); // 瞬间将标志位染红！
            }

            @Override
            public void onDisconnect(InetSocketAddress address) {
                // 毫秒级触发：一旦发现有节点断开连接
                log.error("[RedisFailover] 🚨 监听到 Redis 节点物理断开，目标节点: {}", address);
                topologyDirtyFlag.set(true); // 瞬间将标志位染红！
            }
        });
    }*/

    // ─────────────────────────────────────────────────────────
    // 看门狗主循环
    // ─────────────────────────────────────────────────────────

    /**
     * 定时看门狗：每 5 秒执行一次三探针检测。
     *
     * <p><b>三探针决策矩阵</b>：
     * <pre>
     * epochLost | heartbeatLost | topoDirty | 结论
     * ----------|---------------|-----------|----------------------------------
     *     true  |      any      |    any    | 全量数据丢失，必须升级 epoch
     *    false  |     true      |    any    | 从节点晋升，保守升级 epoch
     *    false  |    false      |    true   | 拓扑变动，需进一步确认（心跳正常则跳过）
     *    false  |    false      |   false   | 完全正常，刷新心跳并重置脏标志
     * </pre>
     */
    @Scheduled(fixedDelay = WATCHDOG_INTERVAL_MS)
    public void cronWatchdog() {
        // 防止重叠执行
        if (!isProcessing.compareAndSet(false, true)) {
            return;
        }

        try {
            String epochVal  = null;
            String heartbeat = null;

            try {
                epochVal  = redisTemplate.opsForValue().get(RedisConstants.LUA_EPOCH);
                heartbeat = redisTemplate.opsForValue().get(RedisConstants.REDIS_HEARTBEAT_KEY);
            } catch (Exception e) {
                // Redis 调用异常：可能正处于 Failover 震荡期，标记脏并等待下一轮
                log.warn("[RedisFailover] 探针读取异常（Redis 可能正在 Failover 中），" +
                        "标记脏状态，等待下一轮检测：{}", e.getMessage());
                topologyDirtyFlag.set(true);
                return;
            }

            boolean epochLost      = epochVal == null;
            boolean heartbeatLost  = heartbeat == null;
            boolean topoDirty      = topologyDirtyFlag.get();

            // 决策：任一探针触发 → 执行升级
            boolean needHeal = epochLost
                    || heartbeatLost
                    || topoDirty;   // 拓扑变动（心跳正常时大概率是连接池复用，Lua 内 doneKey 会拦截重复）

            if (needHeal) {
                log.warn("[RedisFailover] 检测到异常状态，触发自愈：" +
                                "epochLost={}, heartbeatLost={}, topoDirty={}",
                        epochLost, heartbeatLost, topoDirty);
                boolean upgraded = executeEpochUpgrade();
                if (upgraded) {
                    log.warn("[RedisFailover] ✅ epoch 升级成功，Failover 已自愈");
                    meterRegistry.counter("redis_failover_healed_total",
                            "reason", resolveReason(epochLost, heartbeatLost)).increment();
                } else {
                    log.info("[RedisFailover] 其他实例已处理本次 Failover，本实例跳过");
                }
                // 无论是否抢到锁，本轮 Failover 已被处理，重置脏标志
                topologyDirtyFlag.set(false);
            } else {
                // 完全正常：刷新心跳
                heartbeatRefresher.refresh();
                // 若之前因短暂抖动设置了脏标志但无实际影响，顺手重置
                if (topoDirty) {
                    topologyDirtyFlag.set(false);
                }
            }

        } catch (Exception e) {
            log.error("[RedisFailover] Watchdog 执行异常", e);
            // 不重置 topologyDirtyFlag，下一轮继续尝试
        } finally {
            isProcessing.set(false);
        }
    }

    // ─────────────────────────────────────────────────────────
    // 核心：原子 epoch 升级（供 EpochInitializer 和看门狗共用）
    // ─────────────────────────────────────────────────────────

    /**
     * 原子执行 epoch 升级 Lua 脚本。
     *
     * <p>防重机制（双重保障）：
     * <ol>
     *   <li>Lua 内 doneKey 检测：同一次 Failover 只处理一次</li>
     *   <li>Java 侧 isProcessing：同一实例内防止看门狗重叠执行</li>
     * </ol>
     *
     * @return true = 本实例成功执行了 epoch 升级；false = 其他实例已处理
     */
    public boolean executeEpochUpgrade() {
        // 升级前先从 MySQL 读取安全初始值（用于 epochKey 完全消失的场景）
        Long maxMysqlEpoch = stockVersionService.getMaxMysqlEpoch();
        long initEpoch = (maxMysqlEpoch == null ? 0L : maxMysqlEpoch) + 1;

        List<String> keys = Arrays.asList(
                //RedisConstants.FAILOVER_LOCK_KEY,   // KEYS[1] 分布式锁,在lua脚本中执行是伪命题
                RedisConstants.LUA_EPOCH,           // KEYS[1] epoch key
                RedisConstants.FAILOVER_DONE_KEY    // KEYS[2] 已完成标记
        );

        Long result = redisTemplate.execute(
                FAILOVER_LUA_SCRIPT,
                keys,
                String.valueOf(LOCK_TTL_SECONDS),   // ARGV[1] lockTTL
                String.valueOf(initEpoch)            // ARGV[2] initEpoch
        );

        if (result == null) {
            log.error("[RedisFailover] Lua 脚本返回 null，Redis 可能仍在恢复中");
            return false;
        }

        if (result == 0L) {
            // doneKey 存在：其他实例已处理
            return false;
        }

        // result > 0：本实例成功升级，result 即为新 epoch 值
        log.warn("[RedisFailover] 🚀 epoch 原子升级成功，newEpoch={}", result);
        // 升级成功后立即刷新心跳，避免下一个 5s 周期再次误判
        heartbeatRefresher.refresh();
        return true;
    }

    // ─────────────────────────────────────────────────────────
    // 工具方法
    // ─────────────────────────────────────────────────────────

    private String resolveReason(boolean epochLost, boolean heartbeatLost) {
        if (epochLost) return "EPOCH_KEY_MISSING";
        if (heartbeatLost) return "HEARTBEAT_MISSING";
        return "TOPOLOGY_DIRTY";
    }
}