/*
package com.hmall.trade.listener.rocket;

import com.alibaba.fastjson.JSON;
import com.hmall.trade.domain.dto.CancelExecutionContext;
import com.hmall.trade.domain.enmu.OrderStatusEnum;
import com.hmall.trade.domain.po.Order;
import com.hmall.trade.service.IOrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

*/
/**
 * 取消订单事务监听器。
 *
 * 绑定 cancelRocketMQTemplate，与下单监听器完全隔离。
 * 本地事务：更新订单状态为 CANCELLED + 写 cancel_flag 到 Redis。
 * 反查凭证：cancel_flag:{orderId}，TTL=24h。
 *//*

@Slf4j
@Component
@RocketMQTransactionListener(rocketMQTemplateBeanName = "CancelRocketMQTemplate")
@RequiredArgsConstructor
public class CancelOrderTransactionListener implements RocketMQLocalTransactionListener {

    private final IOrderService orderService;
    private final StringRedisTemplate stringRedisTemplate;

    @Override
    public RocketMQLocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        CancelExecutionContext ctx = (CancelExecutionContext) arg;
        Long orderId = ctx.getOrderId();

        try {
            // 本地事务：更新订单状态
            boolean updated = orderService.updateStatusToCancelled(orderId);
            if (!updated) {
                // 【业务失败】：假设 updated=false 是因为订单状态不对（比如已经发货了），这种不需要重试，直接回滚消息
                log.warn("[取消事务] 订单状态流转失败，ROLLBACK。orderId={}", orderId);
                return RocketMQLocalTransactionState.ROLLBACK;
            }

            // 写反查凭证（与订单状态更新不在同一个事务，但业务上可接受：
            // 即使 flag 没写成功，反查时从 DB 读订单状态也能正确判断）
            String flagKey = "cancel:flag:" + orderId;
            stringRedisTemplate.opsForValue().set(flagKey, "1",
                    24, java.util.concurrent.TimeUnit.HOURS);

            log.info("[取消事务] 本地事务成功，COMMIT。orderId={}", orderId);
            return RocketMQLocalTransactionState.COMMIT;

        } catch (Exception e) {
            // 【系统异常】(重点！)：数据库连接超时、死锁等。
            // 此时绝对不能 ROLLBACK！因为我们需要保证它最终成功。
            // 返回 UNKNOWN，让 Broker 等会儿来反查触发重试。
            log.error("[取消事务] 本地事务异常，UNKNOWN。orderId={}", orderId, e);
            return RocketMQLocalTransactionState.UNKNOWN;
        }
    }

    @Override
    public RocketMQLocalTransactionState checkLocalTransaction(Message msg) {
        String orderId = (String) msg.getHeaders().get("cancel_order_id");
        log.warn("[取消订单反查] Broker 反查。orderId={}", orderId);
        try {
            // 优先查 Redis flag（快）
            String flag = stringRedisTemplate.opsForValue().get("cancel:flag:" + orderId);
            if ("1".equals(flag)) {
                return RocketMQLocalTransactionState.COMMIT;
            }

            // flag 不存在，降级查 DB（慢但可靠）
            Order order = orderService.getById(Long.parseLong(orderId));
            if (order != null && (
                    order.getStatus() == OrderStatusEnum.CANCELLED.getCode() ||
                            order.getStatus() == OrderStatusEnum.CLOSED.getCode())) {
                return RocketMQLocalTransactionState.COMMIT;
            }

            // 3. 确认本地事务未执行 → 立即 ROLLBACK
            // 让 XXL-Job 扫到后重新触发 cancelOrderAndRestore 发新消息
            // 比 UNKNOWN 等待15分钟再被强制 ROLLBACK 更快收敛
            log.warn("[取消反查] 本地事务未执行，ROLLBACK。交由 XXL-Job 兜底。orderId={}", orderId);
            return RocketMQLocalTransactionState.ROLLBACK;
        } catch (Exception e) {
            // 注意：只有这里才应该返回 UNKNOWN
            // 原因：查询本身抛异常，说明无法判断状态（DB 宕机、网络超时等）
            // 此时确实不知道本地事务是否执行了，等待下次反查是合理的
            log.error("[取消反查] 查询异常，无法判断状态，UNKNOWN。orderId={}", orderId, e);
            return RocketMQLocalTransactionState.UNKNOWN;
        }
            */
/*//*
/ 3. 【精髓：在反查中重试本地事务】
            // 如果走到这里，说明之前的 executeLocalTransaction 抛异常没执行成功。
            // 既然你要求“必须成功”，那我们就借用反查的线程，再尝试执行一次！
            log.info("[取消反查] 发现本地事务未完成，开始重试本地事务！orderId={}", orderId);
            boolean retryUpdated = orderService.updateStatusToCancelled(Long.parseLong(orderId));

            if (retryUpdated) {
                // 重试成功！补写 Redis 凭证，并提交消息
                stringRedisTemplate.opsForValue().set("cancel:flag:" + orderId, "1", 24, TimeUnit.HOURS);
                log.info("[取消反查] 本地事务重试成功，COMMIT。orderId={}", orderId);
                return RocketMQLocalTransactionState.COMMIT;
            } else {
                // 重试后发现业务条件依旧不满足，只能回滚
                return RocketMQLocalTransactionState.ROLLBACK;
            }

        } catch (Exception e) {
            // 反查重试过程中数据库依然连不上？没关系，继续返回 UNKNOWN。
            // RocketMQ 默认会以时间(60秒)反查 15 次，足够熬过数据库重启或网络抖动。
            log.error("[取消反查] 重试本地事务依然发生系统异常，继续 UNKNOWN 等待下次反查。orderId={}", orderId, e);
            return RocketMQLocalTransactionState.UNKNOWN;
        }*//*

    }
}*/
