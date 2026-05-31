//package com.hmall.trade.listener.rocket;
//
//import com.alibaba.fastjson.JSON;
//import com.hmall.api.dto.OrderDetailDTO;
//import com.hmall.common.utils.UserContext;
//import com.hmall.trade.constants.RedisConstants;
//import com.hmall.trade.domain.dto.OrderFormDTO;
//import com.hmall.trade.domain.po.Order;
//import com.hmall.trade.service.IOrderService;
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
//import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
//import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
//import org.apache.rocketmq.spring.support.RocketMQHeaders;
//import org.springframework.data.redis.core.RedisTemplate;
//import org.springframework.data.redis.core.StringRedisTemplate;
//import org.springframework.messaging.Message;
//import org.springframework.stereotype.Component;
//
//import java.util.Map;
//
///**
// * 订单落库事务监听器。
// * 绑定 DbOrderRocketMQTemplate。
// * 本地事务：handleDbOrder（纯 DB 写入，无 MQ、无消息表）。
// * COMMIT 后 RocketMQ 广播给 item/cart/delay 三个消费者组。
// */
//@Slf4j
//@Component
//@RocketMQTransactionListener(rocketMQTemplateBeanName = "DbOrderRocketMQTemplate")
//@RequiredArgsConstructor
//public class DbOrderTransactionListener implements RocketMQLocalTransactionListener {
//
//    private final IOrderService orderService;
//    private final StringRedisTemplate stringRedisTemplate;
//
//    @Override
//    @SuppressWarnings("unchecked")
//    public RocketMQLocalTransactionState executeLocalTransaction(Message msg, Object arg) {
//        Map<String, Object> msgMap = (Map<String, Object>) arg;
//        String orderIdStr = String.valueOf(msgMap.get("orderId"));
//        Long orderId = Long.parseLong(orderIdStr);
//
//        try {
//            Long userId = Long.valueOf(String.valueOf(msgMap.get("userId")));
//            OrderFormDTO orderFormDTO = JSON.parseObject(
//                    JSON.toJSONString(msgMap.get("orderForm")), OrderFormDTO.class);
//
//            // 设置用户上下文（handleDbOrder 内部可能需要）
//            UserContext.setUser(userId);
//
//            // 纯 DB 落库（不写 LocalEventOutbox，不发任何 MQ）
//            orderService.handleDbOrder(orderId, userId, orderFormDTO);
//
//            log.info("[落库事务] handleDbOrder 成功，COMMIT。orderId={}", orderId);
//            return RocketMQLocalTransactionState.COMMIT;
//
//        } catch (IllegalStateException | IllegalArgumentException e) {
//            // 业务异常（商品不存在、参数非法等），重试无意义，直接 ROLLBACK
//            log.error("[落库事务] 业务异常，ROLLBACK。orderId={}，msg={}", orderId, e.getMessage());
//            return RocketMQLocalTransactionState.ROLLBACK;
//
//        } catch (Exception e) {
//            // 系统异常（DB 宕机等），状态未知，UNKNOWN 等 Broker 反查
//            log.error("[落库事务] 系统异常，UNKNOWN。orderId={}", orderId, e);
//            return RocketMQLocalTransactionState.UNKNOWN;
//
//        } finally {
//            UserContext.removeUser();
//        }
//    }
//
//    @Override
//    public RocketMQLocalTransactionState checkLocalTransaction(Message msg) {
//        String orderId = (String) msg.getHeaders().get(RocketMQHeaders.KEYS);
//        log.warn("[落库反查] Broker 反查。orderId={}", orderId);
//
//        try {
//            Order order = orderService.getById(Long.parseLong(orderId));
//            if (order != null) {
//                // 订单已落库 → COMMIT，让广播消息投递给下游
//                log.info("[落库反查] 订单已存在，COMMIT。orderId={}", orderId);
//                return RocketMQLocalTransactionState.COMMIT;
//            }
//            // 2. 【大厂核心防御】MySQL 没有，不能盲目 ROLLBACK！必须去 Redis 查有没有扣减凭证
//            // 假设你在 Lua 脚本扣减成功时，在 Redis 里写了一个临时凭证（TTL=24小时）
//            String flagKey = RedisConstants.LUA_ORDER_FLAG_PREFIX + orderId;
//            Boolean hasRedisToken = stringRedisTemplate.hasKey(flagKey);
//
//            if (Boolean.TRUE.equals(hasRedisToken)) {
//                // 证明：用户确实在 Redis 抢到了资格！只是刚才 MySQL 挂了导致本地事务没进去
//                log.warn("[落库反查拦截] MySQL 无单但 Redis 凭证存在！触发高可用兜底。orderId={}", orderId);
//
//                // 策略 A：返回 UNKNOWN，让 Broker 10-30 秒后再来反查，拖延时间等待 MySQL 恢复
//                // 策略 B（更硬核）：直接返回 COMMIT！让消息放行投递给下游。
//                //                 同时，在这个反查线程里，或者由下游消费者发现“主订单不存在”时，
//                //                 利用消息体（Payload）里完整的 orderFormDTO 实施“逆向强制补全建单”。
//                return RocketMQLocalTransactionState.UNKNOWN;
//            }
//            // 3. 只有当 MySQL 没有，且 Redis 里也没有扣减凭证（说明 Lua 压根没过），才安全地 ROLLBACK
//            log.warn("[落库反查] 订单不存在，ROLLBACK。orderId={}", orderId);
//            return RocketMQLocalTransactionState.ROLLBACK;
//
//        } catch (Exception e) {
//            // 查询异常（DB 还未恢复），继续 UNKNOWN 等待下次反查
//            log.error("[落库反查] 查询异常，UNKNOWN。orderId={}", orderId, e);
//            return RocketMQLocalTransactionState.UNKNOWN;
//        }
//    }
//}