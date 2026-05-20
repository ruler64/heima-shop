package com.hmall.trade.listener.rocket;

import com.hmall.api.client.ItemClient;
import com.hmall.common.exception.BizIllegalException;
import com.hmall.trade.constants.RedisConstants;
import com.hmall.trade.domain.dto.LuaExecutionContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RocketMQTransactionListener(rocketMQTemplateBeanName = "LuaRocketMQTemplate")
@RequiredArgsConstructor
public class OrderTransactionListener implements RocketMQLocalTransactionListener {

    private final StringRedisTemplate stringRedisTemplate;
    private final ItemClient itemClient;  // Feign 调 item-service 触发懒加载

    @Qualifier("deductStockAndSaveMsgScript")
    private final DefaultRedisScript<Long> deductStockAndSaveMsgScript;

    /**
     * 半消息发送成功后立即回调：执行 Lua 扣减库存。
     *
     * <p>返回值处理策略：
     * <ul>
     *   <li> 0    → COMMIT（含幂等放行）</li>
     *   <li>-99   → 全局 epoch 丢失，BizIllegalException 向上抛，外层捕获后 ROLLBACK</li>
     *   <li>-(i)  → 第 i 个商品库存 key 缺失，懒加载后单次重试</li>
     *   <li>+(i)  → 第 i 个商品库存真实不足，ROLLBACK</li>
     *   <li>异常  → UNKNOWN，等 Broker 反查</li>
     * </ul>
     */
    @Override
    public RocketMQLocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        LuaExecutionContext ctx = (LuaExecutionContext) arg;
        String orderId = ctx.getOrderId();

        try {
            Long result = stringRedisTemplate.execute(
                    deductStockAndSaveMsgScript,
                    ctx.getKeys(),
                    ctx.getArgs()
            );
            return handleLuaResult(result, ctx, orderId, false);
        } catch (BizIllegalException e) {
            log.error("[RocketMQ事务] 业务异常，ROLLBACK。orderId={}, msg={}", orderId, e.getMessage());
            return RocketMQLocalTransactionState.ROLLBACK;
        } catch (Exception e) {
            // Redis异常：状态未知，等Broker来反查
            log.error("[RocketMQ事务] Lua执行异常，进入UNKNOWN。orderId={}", orderId, e);
            return RocketMQLocalTransactionState.UNKNOWN;//broker按兵不动，等待反查
        }
    }

    /**
     * Broker反查：服务宕机重启后，Broker主动调此方法确认状态
     * 默认60s反查一次，最多15次
     * RocketMQ默认60秒后才发起第一次反查，正常情况下Lua早已执行完毕。
     * 10秒阈值只是为了防止极端情况（Redis主从切换期间Lua执行超长），实际上反查到来时Lua几乎必然已经结束，
     * 这个判断是一个保险层而非主要逻辑。
     */
    @Override
    public RocketMQLocalTransactionState checkLocalTransaction(Message msg) {
        String orderId = (String) msg.getHeaders().get(RocketMQHeaders.TRANSACTION_ID);
        log.warn("[RocketMQ反查] 触发Broker反查。orderId={}", orderId);
        // flag不存在，判断是"反查过早"还是"真正失败"
        String createTimeStr = (String) msg.getHeaders().get("order_create_time");
        if (createTimeStr != null) {
            long createTime = Long.parseLong(createTimeStr);
            long elapsed = System.currentTimeMillis() - createTime;

            // 10秒内查不到flag，认为Lua还在执行，继续等待
            if (elapsed < 10_000L) {
                log.warn("[RocketMQ反查] 距创建仅{}ms，Lua可能仍在执行，返回UNKNOWN等待下次反查。orderId={}", elapsed, orderId);
                return RocketMQLocalTransactionState.UNKNOWN;
            }
        }
        // flag key 与库存 key 同在 {stock} hash tag，原子性保证：
        //   flag 存在  ↔ 库存已扣减  → COMMIT
        //   flag 不存在（超 10s）：
        //     情况 A：Lua 根本没执行（库存未扣）→ 安全 ROLLBACK
        //     情况 B：主从切换 flag 与库存一起丢（两边状态一致）→ 安全 ROLLBACK
        String flagKey = "order:flag:{stock}:" + orderId;
        String flag = stringRedisTemplate.opsForValue().get(flagKey);

        if ("1".equals(flag)) {
            log.info("[RocketMQ反查] flag存在，COMMIT。orderId={}", orderId);
            return RocketMQLocalTransactionState.COMMIT;
        } else {
            // 超过10秒还没有flag，两种情况都安全ROLLBACK：
            // 1. Lua根本没执行（库存没扣）
            // 2. 主从切换丢了flag，但库存扣减同样丢了，两边一致
            log.info("[RocketMQ反查] flag不存在，ROLLBACK。orderId={}", orderId);
            return RocketMQLocalTransactionState.ROLLBACK;
        }
    }

    /**
     * 统一处理 Lua 返回值，isRetry 防止懒加载递归。
     */
    private RocketMQLocalTransactionState handleLuaResult(
            Long result, LuaExecutionContext ctx, String orderId, boolean isRetry) {

        if (result == null) {
            log.error("[RocketMQ事务] Lua 返回 null，进入 UNKNOWN。orderId={}", orderId);
            return RocketMQLocalTransactionState.UNKNOWN;
        }

        // ① 成功（含幂等放行）
        if (result == 0L) {
            log.info("[RocketMQ事务] Lua 扣减成功，COMMIT。orderId={}", orderId);
            return RocketMQLocalTransactionState.COMMIT;
        }

        // ② 全局 epoch 丢失：EpochInitializer 还未写入的极短窗口（ApplicationRunner 在接受请求前跑完，
        //    实际上几乎不可能在这里触发，作为最后保险）
        if (result == -99L) {
            log.error("[RocketMQ事务] Redis epoch 运行时丢失，拒绝下单。orderId={}", orderId);
            throw new BizIllegalException("系统繁忙，请稍后重试");
        }

        // ③ 正数：库存真实不足，直接拒单
        if (result > 0) {
            log.warn("[RocketMQ事务] 库存不足，ROLLBACK。orderId={}，失败商品 index={}", orderId, result);
            return RocketMQLocalTransactionState.ROLLBACK;
        }

        // ④ 负数（非 -99）：第 |result| 个商品的库存 key 不存在（Redis 未预热）
        //    懒加载策略：调 item-service 从 MySQL 写库存到 Redis，然后单次重试
        //    注意：只重试一次（isRetry 标志防死循环）
        if (!isRetry) {
            int missingIndex = (int) (-result) - 1;   // 转为 0-based 下标
            Long missingItemId = ctx.getItemIds().get(missingIndex);
            log.warn("[RocketMQ事务] 库存 key 缺失，触发懒加载。orderId={}，itemId={}", orderId, missingItemId);

            try {
                itemClient.loadStockToRedis(missingItemId);
                log.info("[RocketMQ事务] 懒加载完成，开始单次重试。orderId={}，itemId={}", orderId, missingItemId);
                Long retryResult = executeLua(ctx);
                return handleLuaResult(retryResult, ctx, orderId, true);  // isRetry=true

            } catch (Exception e) {
                log.error("[RocketMQ事务] 懒加载或重试失败，保守 ROLLBACK。orderId={}，itemId={}", orderId, missingItemId, e);
                return RocketMQLocalTransactionState.ROLLBACK;
            }
        }

        // isRetry=true 仍拿到负数：极罕见（懒加载成功写入 Redis 但立即被驱逐？），保守 ROLLBACK
        log.error("[RocketMQ事务] 重试后库存 key 仍缺失，保守 ROLLBACK。orderId={}", orderId);
        return RocketMQLocalTransactionState.ROLLBACK;
    }

    private Long executeLua(LuaExecutionContext ctx) {
        return stringRedisTemplate.execute(deductStockAndSaveMsgScript, ctx.getKeys(), ctx.getArgs());
    }
}