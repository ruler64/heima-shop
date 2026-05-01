package com.hmall.trade.listener;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.hmall.common.exception.BadRequestException;
import com.hmall.common.utils.UserContext;
import com.hmall.trade.constants.MQConstants;
import com.hmall.trade.domain.dto.OrderFormDTO;
import com.hmall.trade.service.impl.OrderServiceImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.*;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Component;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.Message;

import java.io.IOException;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderMQListener {

    private final OrderServiceImpl orderService;

    private Long toLong(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return Long.valueOf(String.valueOf(value));
    }

    @RabbitListener(
            concurrency = "16-64",
            bindings = @QueueBinding(
                    value = @Queue(name = MQConstants.ASYNC_ORDER_QUEUE, durable = "true"),
                    exchange = @Exchange(name = MQConstants.ASYNC_ORDER_EXCHANGE),
                    key = MQConstants.ASYNC_ORDER_KEY
            )
    )
    public void listenAsyncOrder(Map<String, Object> msg, Channel channel, Message message) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();//MQ消息的递送标签,message是壳子并不是实际数据
        Long orderId = null;
        /*
        * 场景 A（超时其实成功了）： 订单 DB 插入成功 -> 调用库存 RPC -> 库存服务其实扣减成功了，但网络卡了没按时返回 ->
        * 你的代码抛出 RuntimeException -> 订单 DB 回滚。结果： 此时库存扣了，订单没了。虽然依靠 MQ 的重试和
        * 我们之前设计的强幂等库存接口，下一次重试能把订单补回来。但这只是侥幸。
        * 场景 B（终极噩梦：宕机）：订单 DB 插入成功 -> 调用库存 RPC 扣减成功 -> 就在这一瞬间，订单服务的 JVM 突然断电宕机
        * （或者 OOM）。结果： MySQL 事务因为断开连接自动回滚（订单没存下来）。RabbitMQ 发现消费者掉线，消息重新入队。
        * 等订单服务重启后，MQ 再次投递消息。
        *
        * 宕机时，catch 块里的 delete(key) 根本没执行！这个 Redis 锁会卡死 24 小时。
            重启后，重试的消息进来发现 isFirstTime 是 false，直接就把消息 ACK（丢弃）了！
            最终结果：库存被扣了，订单彻底丢了，消息也没了，发生严重的资损！
        * */
        try {
            orderId = toLong(msg.get("orderId"));
            Long userId = toLong(msg.get("userId"));
            OrderFormDTO orderFormDTO = JSON.parseObject(JSON.toJSONString(msg.get("orderForm")), OrderFormDTO.class);
            Long epoch = toLong(msg.get("epoch"));
            Long seq = toLong(msg.get("seq"));
            String version = msg.get("version") == null ? null : String.valueOf(msg.get("version"));
            if (log.isDebugEnabled()) {
                log.debug("开始消费异步下单消息，orderId={}, userId={}, version={}", orderId, userId, version);
            }
            UserContext.setUser(userId);
            // 直接调用落库，防重交给 DB 的唯一索引
            orderService.handleDbOrder(orderId, userId, orderFormDTO, epoch, seq, version);
            channel.basicAck(deliveryTag, false);
            if (log.isDebugEnabled()) {
                log.debug("异步下单消息消费成功并ACK，orderId={}", orderId);
            }
        } catch (DuplicateKeyException e) {
            // 如果报主键冲突，说明已经被成功消费过了，直接放行
            log.warn("订单 {} 已存在，判定为重复消费兜底成功，直接 ACK。", orderId);
            channel.basicAck(deliveryTag, false);
        } catch (BadRequestException | JSONException e) {
            // 1. 业务型/不可恢复异常：比如商品不存在、解析错误。重试没用，直接丢弃或进入死信队列
            log.error("发生不可恢复的业务异常，拒绝消息（扔进死信队列）。订单号: {}", orderId, e);
            channel.basicNack(deliveryTag, false, false); // 注意：最后一个参数是 false！
        }catch (Exception e) {
            log.error("订单异步落库发生异常，准备触发 MQ 重试。订单号: {}", orderId, e);
            // 发生业务/网络异常，无条件 NACK 让 MQ 不断重试
            channel.basicNack(deliveryTag, false, true);
        } finally {
            UserContext.removeUser();
        }


        /*
        // 1. 构建幂等性 Key
        String idempotentKey = "idempotent:order:" + orderId;
        try {
            // 2. 第一层防御：利用 Redis SETNX 尝试加锁防重
            Boolean isFirstTime = stringRedisTemplate.opsForValue()
                    .setIfAbsent(idempotentKey, "PROCESSING", 24, TimeUnit.HOURS);

            if (Boolean.FALSE.equals(isFirstTime)) {
                // 【大厂细节 1】发现重复消息，拦截后必须手动 ACK 告诉 MQ 删除它，而不是简单 return
                log.warn("检测到重复投递的下单消息，Redis层拦截成功！订单号: {}", orderId);
                channel.basicAck(deliveryTag, false);
                return;
            }
            // 设置当前线程的用户上下文
            UserContext.setUser(userId);
            // 3. 执行真正的落库逻辑 (此方法内部必须有 @Transactional 保证本地事务)
            orderService.handleDbOrder(orderId, userId, orderFormDTO);

            // 【大厂细节 2】落库成功后，执行真正的手动 ACK
            channel.basicAck(deliveryTag, false);
            log.info("订单异步落库成功，已手动ACK。订单号: {}", orderId);

        } catch (DuplicateKeyException e) {
            // 【大厂细节 3：第二层兜底防御】
            // 什么时候会走到这里？Redis 的 Key 被清了/过期了，但数据库其实已经有这个订单了。
            // 这时触发唯一索引异常。既然数据库已经有了，说明业务已经成功了，我们要把这个重复的锅背下来，直接 ACK 掉！
            log.warn("触发数据库唯一索引异常，判定为重复消费兜底成功，直接 ACK。订单号: {}", orderId);
            // 千万不能 delete Redis Key，直接 ACK 即可
            channel.basicAck(deliveryTag, false);

        } catch (Exception e) {
            // 【大厂细节 4】真正的业务异常（比如余额不足、网络异常等）
            log.error("订单异步落库发生业务异常，准备触发重试。订单号: {}", orderId, e);
            // 必须删除幂等 Key！这样 RabbitMQ 触发重试机制再次投递时，才能通过 Redis 的拦截
            stringRedisTemplate.delete(idempotentKey);

            // 执行手动 NACK，让消息重回队列 (requeue = true)
            channel.basicNack(deliveryTag, false, true);
        } finally {
            UserContext.removeUser();
        }
        */
    }
}