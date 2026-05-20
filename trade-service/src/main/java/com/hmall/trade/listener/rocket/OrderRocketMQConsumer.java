package com.hmall.trade.listener.rocket;

import com.alibaba.fastjson.JSON;
import com.hmall.common.exception.BizIllegalException;
import com.hmall.common.utils.UserContext;
import com.hmall.trade.constants.MQConstants;
import com.hmall.trade.constants.RedisConstants;
import com.hmall.trade.domain.dto.OrderFormDTO;
import com.hmall.trade.service.IOrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * 不发消息，考虑直接在该消费者直接生成订单。考虑改造半事务消息的消费者组的数量，给自己发延迟消息，item扣减库存，cart清空购物车
 * 半事务消息redis预扣成功commit->下游消费者：执行本地事务、给自己发延迟消息，item扣减库存，cart清空购物车
 * 四个消费者组监听同一个TOPIC，这样就可以保证解决重试逻辑所在两阶段提交的位置问题
 */
@Slf4j
@Service
@RocketMQMessageListener(
        topic = MQConstants.ROCKETMQ_LUA_TOPIC,
        consumerGroup = MQConstants.ROCKETMQ_LUA_CONSUMER_GROUP,
        // 对应RabbitMQ的 concurrency = "16-64"
        consumeThreadNumber = 8,        // 消费线程数（固定值，RocketMQ不支持动态扩缩）
        // 消息拉取间隔
        consumeTimeout = 15000L,         // 消费超时15秒，超时视为失败触发重试
        // 最大重试次数，超过后进入死信Topic（%DLQ%trade-order-consumer-group）
        maxReconsumeTimes = 5   // 阶梯重试5次，2分钟内收敛
)
@RequiredArgsConstructor
public class OrderRocketMQConsumer implements RocketMQListener<String> {

    private final IOrderService orderService;
    private final StringRedisTemplate stringRedisTemplate;

    @Override
    public void onMessage(String msgJson) {
        Map<?, ?> msg = JSON.parseObject(msgJson, Map.class);
        Long orderId = Long.valueOf(String.valueOf(msg.get("orderId")));
        Long userId = Long.valueOf(String.valueOf(msg.get("userId")));
        OrderFormDTO orderFormDTO = JSON.parseObject(JSON.toJSONString(msg.get("orderForm")), OrderFormDTO.class);

        log.info("[建单消费] 开始落库。orderId={}", orderId);

        try {
            UserContext.setUser(userId);
            orderService.handleDbOrder(orderId, userId, orderFormDTO);

            //此处如果直接删除LUA写入redis的幂等key的话，可能会导致重复扣减redis库存的问题，是否应该考虑24小时后删除？
            /*long bucket = Math.floorMod(orderId, (long) RedisConstants.OUTBOX_BUCKETS);
            String outboxKey = RedisConstants.OUTBOX_KEY_PREFIX + bucket;
            stringRedisTemplate.opsForHash().delete(outboxKey, String.valueOf(orderId));*/

            log.info("[建单消费] 落库成功。orderId={}", orderId);
        } catch (org.springframework.dao.DuplicateKeyException e) {
            log.warn("[建单消费] 幂等放行。orderId={}", orderId);
        } catch (BizIllegalException e) {
            log.error("[建单消费] 业务异常，进DLQ。orderId={}，{}", orderId, e.getMessage());
            throw new RuntimeException(e);
        } catch (Exception e) {
            log.error("[建单消费] 系统异常，触发重试。orderId={}", orderId, e);
            throw new RuntimeException("建单失败，触发重试", e);
        } finally {
            UserContext.removeUser();
        }
    }
}
