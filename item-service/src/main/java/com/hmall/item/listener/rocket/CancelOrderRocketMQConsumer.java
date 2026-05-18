package com.hmall.item.listener.rocket;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hmall.api.dto.OrderDetailDTO;
import com.hmall.item.config.ItemCachePreloader;
import com.hmall.item.constants.MQConstants;
import com.hmall.item.service.IItemService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 取消订单消费者（item-service）。
 *
 * 职责：
 * 1. 恢复 MySQL 库存（increaseStock，带幂等保护）
 * 2. 恢复 Redis 预扣库存（INCRBY，原子操作）
 *
 * 消息来源：trade-service 的 cancelRocketMQTemplate 发出的已 COMMIT 事务消息。
 * 此时 trade-service 的订单状态已变为 CANCELLED，本消费者只负责库存侧的恢复。
 */
@Slf4j
@Service
@RocketMQMessageListener(
        topic = MQConstants.ROCKETMQ_CANCEL_TOPIC,          // 与 MQConstants.ROCKETMQ_CANCEL_TOPIC 一致
        consumerGroup = MQConstants.ROCKETMQ_CANCEL_CONSUMER_GROUP,
        consumeThreadNumber = 8,
        consumeTimeout = 15000L,
        maxReconsumeTimes = 16
)
@RequiredArgsConstructor
public class CancelOrderRocketMQConsumer implements RocketMQListener<String> {

    private final IItemService itemService;
    private final StringRedisTemplate stringRedisTemplate;

    @Override
    public void onMessage(String msgJson) {
        JSONObject json = JSON.parseObject(msgJson);
        Long orderId = json.getLong("orderId");
        List<OrderDetailDTO> details = json.getJSONArray("details")
                .toJavaList(OrderDetailDTO.class);

        log.info("[取消消费] 收到订单取消消息，开始恢复库存。orderId={}", orderId);

        // Step 1：恢复 MySQL 库存（increaseStock 内部有幂等保护，重复消费安全）
        try {
            itemService.increaseStock(orderId, details);
            log.info("[取消消费] MySQL 库存恢复成功。orderId={}", orderId);
        } catch (Exception e) {
            // MySQL 恢复失败，抛异常触发 RocketMQ 重试
            // 不能跳过 MySQL 直接去恢复 Redis，否则两边不一致
            log.error("[取消消费] MySQL 库存恢复失败，触发重试。orderId={}", orderId, e);
            throw new RuntimeException("MySQL 库存恢复失败", e);
        }

        // Step 2：恢复 Redis 预扣库存
        // MySQL 已成功，Redis 尽力恢复，失败不重试（凌晨对账兜底）
        try {
            restoreRedisStock(orderId, details);
            log.info("[取消消费] Redis 库存恢复成功。orderId={}", orderId);
        } catch (Exception e) {
            // Redis 恢复失败只打日志，不抛异常（不影响 RocketMQ ACK）
            // 凌晨对账任务会检测到 Redis < MySQL，自动修复
            log.error("[取消消费] Redis 库存恢复失败，等待凌晨对账修复。orderId={}", orderId, e);
        }
    }

    /**
     * 恢复 Redis 预扣库存。
     * 直接 INCRBY，不需要 Lua 脚本（恢复操作天然幂等：多次加回来会超过 MySQL，
     * 凌晨对账检测到 Redis > MySQL 会用 MySQL 覆盖，最终收敛）。
     */
    private void restoreRedisStock(Long orderId, List<OrderDetailDTO> details) {
        for (OrderDetailDTO detail : details) {
            String stockKey = ItemCachePreloader.ITEM_STOCK_KEY_PREFIX + detail.getItemId();

            // 仅当 Redis 中该 key 存在时才恢复，不存在说明未预热，跳过
            // （不存在时 INCRBY 会创建 key 并赋值，会造成脏数据）
            Boolean exists = stringRedisTemplate.hasKey(stockKey);
            if (Boolean.TRUE.equals(exists)) {
                stringRedisTemplate.opsForValue().increment(stockKey, detail.getNum());
                log.info("[取消消费] Redis 库存恢复。itemId={}，恢复数量={}",
                        detail.getItemId(), detail.getNum());
            } else {
                log.info("[取消消费] Redis 库存 key 不存在，跳过恢复。itemId={}", detail.getItemId());
            }
        }
    }
}