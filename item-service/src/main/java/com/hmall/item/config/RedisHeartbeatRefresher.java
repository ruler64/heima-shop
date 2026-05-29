package com.hmall.item.config;

import com.hmall.item.constants.RedisConstants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Redis 集群心跳探针刷新器。
 *
 * <p><b>设计意图</b>：
 * Redis Cluster 主从切换（Failover）期间，主节点宕机、从节点未完成选举前，
 * 任何写操作都会失败，心跳 key（TTL=15s）会在此窗口内自然过期。
 * 服务重启或看门狗轮询时，若检测到心跳 key 不存在，
 * 则保守判定为发生了 Failover，触发 epoch 升级流程。
 *
 * <p><b>刷新频率设计</b>：
 * <ul>
 *   <li>心跳 TTL = 15s</li>
 *   <li>刷新间隔 = 5s（3倍关系，确保正常情况下 TTL 不会耗尽）</li>
 *   <li>Failover 窗口 ≈ 10~30s，足以让 TTL 过期，触发探针判定</li>
 * </ul>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RedisHeartbeatRefresher {

    /** 心跳 key TTL，单位：秒。必须 > Failover 最大耗时（通常 10~30s）。 */
    private static final long HEARTBEAT_TTL_SECONDS = 15L;

    /** 刷新间隔，单位：毫秒。 */
    private static final long REFRESH_INTERVAL_MS = 5_000L;

    private final StringRedisTemplate redisTemplate;

    /**
     * 服务就绪后立即写入第一次心跳，避免启动阶段心跳空窗被误判为 Failover。
     * ApplicationReadyEvent 在所有 ApplicationRunner 执行完毕后触发，
     * 保证 EpochInitializer 已经完成 epoch 初始化。
     */
    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        refresh();
        log.info("[Heartbeat] 服务就绪，已写入首次心跳 key，TTL={}s", HEARTBEAT_TTL_SECONDS);
    }

    /**
     * 定时刷新心跳 key。
     * fixedDelay 保证上一次执行完毕后再等 5s，
     * 即使 Redis 出现短暂抖动也不会引发线程堆积。
     */
    @Scheduled(fixedDelay = REFRESH_INTERVAL_MS)
    public void scheduledRefresh() {
        refresh();
    }

    /**
     * 刷新心跳 key 核心逻辑。
     * 写失败只打 WARN，不抛异常，让 RedisFailoverDetector 的看门狗负责感知 Failover。
     */
    public void refresh() {
        try {
            redisTemplate.opsForValue().set(
                    RedisConstants.REDIS_HEARTBEAT_KEY,
                    "1",
                    HEARTBEAT_TTL_SECONDS,
                    TimeUnit.SECONDS
            );
        } catch (Exception e) {
            // 写失败说明 Redis 正在 Failover，打 WARN 即可，不影响主流程
            log.warn("[Heartbeat] 心跳刷新失败，Redis 可能正在 Failover 中：{}", e.getMessage());
        }
    }
}