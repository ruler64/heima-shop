package com.hmall.trade.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.CollectionUtils;
import com.baomidou.mybatisplus.core.toolkit.IdWorker;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmall.api.client.CartClient;
import com.hmall.api.client.ItemClient;
import com.hmall.api.dto.ItemDTO;
import com.hmall.api.dto.OrderDetailDTO;
import com.hmall.common.exception.BadRequestException;
import com.hmall.common.exception.BizIllegalException;
import com.hmall.common.utils.BeanUtils;
import com.hmall.common.utils.RabbitMqHelper;
import com.hmall.common.utils.UserContext;

import com.hmall.trade.constants.LuaConstants;
import com.hmall.trade.constants.MQConstants;
import com.hmall.trade.constants.RedisConstants;
import com.hmall.trade.domain.dto.CancelExecutionContext;
import com.hmall.trade.domain.dto.LuaExecutionContext;
import com.hmall.trade.domain.dto.OrderFormDTO;
import com.hmall.trade.domain.enmu.OrderStatusEnum;
import com.hmall.trade.domain.po.LocalEventOutbox;
import com.hmall.trade.domain.po.Order;
import com.hmall.trade.domain.po.OrderDetail;
import com.hmall.trade.mapper.LocalEventOutboxMapper;
import com.hmall.trade.mapper.OrderMapper;
import com.hmall.trade.service.IOrderDetailService;
import com.hmall.trade.service.IOrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2023-05-05
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class OrderServiceImpl extends ServiceImpl<OrderMapper, Order> implements IOrderService {

    //private final IItemService itemService;
    private final ItemClient itemClient;
    private final RedissonClient redissonClient;
    private final IOrderDetailService detailService;
//    private final CartClient cartClient;
    private final RabbitTemplate rabbitTemplate;
    private final RabbitMqHelper rabbitMqHelper;
    private final RocketMQTemplate rocketMQTemplate;

    @Autowired
    @Qualifier("outboxStatusExecutor")
    private Executor outboxStatusExecutor;

    // 🌟 核心技巧：自己注入自己的代理对象，利用 @Lazy 规避 Spring 的循环依赖检查
    @Autowired
    @Lazy
    private IOrderService orderSelfProxy;

    // 去掉 final，改用 @Resource（按 Bean 名称注入，不需要 @Qualifier）
//    @Resource(name = "CancelRocketMQTemplate")
//    private RocketMQTemplate cancelRocketMQTemplate;

    @Resource(name = "LuaRocketMQTemplate")
    private RocketMQTemplate luaRocketMQTemplate;

    private final StringRedisTemplate stringRedisTemplate;
    private final LocalEventOutboxMapper localEventOutboxMapper;

    /*private String buildOutboxKey(Long orderId) {
        long bucket = Math.floorMod(orderId, RedisConstants.OUTBOX_BUCKETS);
        return RedisConstants.OUTBOX_KEY_PREFIX + bucket;
    }*/

    // ✅ 新增：每订单独立 key
    private String buildIdemKey(Long orderId) {
        return RedisConstants.ORDER_IDEM_KEY_PREFIX + orderId;
    }
    // 1. 初始化 Lua 脚本
    // 【注入配置类中定义的 Lua 脚本】
//    @Qualifier("deductStockAndSaveMsgScript")
//    private final DefaultRedisScript<List> DEDUCT_STOCK_AND_SAVE_MSG_SCRIPT;


    /**
     * 定义一段 Lua 脚本：兼顾【幂等校验】和【批量恢复库存】的原子操作
     * 逻辑：
     * 1. 尝试用 setnx 设置一个退库凭证（防重放）。
     * 2. 如果设置成功（返回1），说明是首次退库，给凭证设置过期时间（防止垃圾数据），然后遍历恢复库存。
     * 3. 如果设置失败（返回0），说明之前已经退过库了，直接无视，返回 0 触发幂等放行。
     */
    private static final String RESTORE_STOCK_LUA =
            "if redis.call('setnx', KEYS[1], '1') == 1 then " +
                    "   redis.call('expire', KEYS[1], 86400) " +  // 凭证保留 24 小时
                    "   for i = 2, #KEYS do " +
                    "       redis.call('incrby', KEYS[i], ARGV[i-1]) " +
                    "   end " +
                    "   return 1 " +
                    "else " +
                    "   return 0 " +
                    "end";


    private static final DefaultRedisScript<Long> RESTORE_SCRIPT = new DefaultRedisScript<>(RESTORE_STOCK_LUA, Long.class);


    @Override
    public Long createOrder(OrderFormDTO orderFormDTO) {
        Long userId = UserContext.getUser();

        // 1. 【提前】生成订单 ID 和组装要发送的 MQ 消息
        Long orderId = IdWorker.getId();
        Map<String, Object> msg = new HashMap<>();
        msg.put("orderId", orderId);
        msg.put("userId", userId);
        msg.put("orderForm", orderFormDTO);
        String msgJson = JSON.toJSONString(msg); // 序列化为 JSON 存入 Redis

        // 2. 准备 Lua 脚本需要的 KEYS 和 ARGV
        List<String> keys = new ArrayList<>();
        List<Long> itemIds = new ArrayList<>();
        Object[] args = new Object[orderFormDTO.getDetails().size() + 1];

        int i = 0;
        for (; i < orderFormDTO.getDetails().size(); i++) {
            OrderDetailDTO detail = orderFormDTO.getDetails().get(i);
            // KEYS[1..n]：库存key
            keys.add(RedisConstants.ITEM_STOCK_KEY_PREFIX + detail.getItemId());
            itemIds.add(detail.getItemId());
            args[i] = String.valueOf(detail.getNum());
        }
        // Redis HASH 无法对单个 field 设置 TTL，随订单增多无限膨胀。Redis outbox：分桶降低单 Key 大 Hash 的体积与热点
//        String outboxKey = buildOutboxKey(orderId);
        String idemKey = buildIdemKey(orderId);
        // 这里仍然沿用 outbox 存储消息，确保 MQ 双写一致性。
        // Lua 脚本只依赖库存 key + outbox key；epoch/seq/version 由 Lua 自己在 Redis 中生成。
//        keys.add(outboxKey);                           // KEYS[n+1] outbox
        keys.add(idemKey);   // KEYS[n+1] idem String key，TTL=24h
        keys.add(RedisConstants.LUA_EPOCH);          // KEYS[n+2] epoch
        //keys.add(RedisConstants.LUA_SEQUENCE);            // 移除全局seq流水号KEYS[n+3] seq
        keys.add(RedisConstants.LUA_ORDER_FLAG_PREFIX + orderId);     // KEYS[n+3] flag
        args[i] = String.valueOf(orderId);          // ARGV[n+1] orderId
        // KEYS[n+4..2n+3]：per-item seq keys（新增）
        for (Long itemId : itemIds) {
            keys.add(RedisConstants.LUA_SEQUENCE + itemId);
        }
        // KEYS[2n+4..3n+3]：per-item ver keys（修复：原来错误地加了stock key）
        for (Long itemId : itemIds) {
            keys.add(RedisConstants.ITEM_STOCK_VERSION_KEY_PREFIX + itemId);
        }
        //args[i + 1] = msgJson;
        // 3. 封装Lua执行上下文，传给事务监听器（新增）
        LuaExecutionContext ctx = LuaExecutionContext.builder()
                .orderId(String.valueOf(orderId))
                .keys(keys)
                .args(args)
                .itemIds(itemIds)
                .build();
        // 4. 构建RocketMQ事务消息
        org.springframework.messaging.Message<String> rocketMsg =
                org.springframework.messaging.support.MessageBuilder
                        .withPayload(msgJson)
                        .setHeader(
                                org.apache.rocketmq.spring.support.RocketMQHeaders.TRANSACTION_ID,
                                String.valueOf(orderId)
                        )
                        .setHeader("order_create_time", String.valueOf(System.currentTimeMillis()))
                        .build();

        // 5. 发送RocketMQ半事务消息
        //    半消息→Broker持久化→回调executeLocalTransaction执行Lua
        //    Lua成功→COMMIT→Broker投递给OrderRocketMQConsumer→转发RabbitMQ
        org.apache.rocketmq.client.producer.TransactionSendResult sendResult =
                luaRocketMQTemplate.sendMessageInTransaction(
                        MQConstants.ROCKETMQ_LUA_TOPIC,
                        rocketMsg,
                        ctx  // 传给executeLocalTransaction的arg参数
                );

        if (!org.apache.rocketmq.client.producer.LocalTransactionState.COMMIT_MESSAGE
                .equals(sendResult.getLocalTransactionState())) {
            log.warn("用户 {} 下单失败，库存不足或Redis异常。orderId={}", userId, orderId);
            throw new BizIllegalException("部分商品库存不足，下单失败！");
        }

        log.info("下单成功，RocketMQ事务消息已提交。orderId={}", orderId);
        return orderId;
        /*// 3. 执行 Lua 脚本，原子性扣减库存 + 保存消息！
        // 此时就算下一秒机器突然断电炸了，只要 Lua 执行成功，Redis 里必定同时有扣减后的库存和这条待发送的消息。
        Long result = stringRedisTemplate.execute(DEDUCT_STOCK_AND_SAVE_MSG_SCRIPT, keys, args);

        if (result != null && !result.equals(LuaConstants.LUA_SUCCESS)) {
            log.warn("用户 {} 下单失败，商品 {} 库存不足", userId, result);
            throw new BizIllegalException("部分商品库存不足，下单失败！");
        }

//        LocalEventOutbox outbox = new LocalEventOutbox();
//        outbox.setOrderId(orderId);
//        outbox.setEventType(ORDER_CREATED_EVENT);
//        outbox.setPayload(msgJson);
//        // MySQL outbox 只负责“消息是否可靠投递”的事实，不承接 Redis 库存版本事实。
//        // 库存 epoch/seq 由 Redis 预扣链路和 item 库存流水分别独立维护，用于后续对账判断。
//        outbox.setSource("MYSQL");
//        outbox.setStatus(0);
//        outbox.setRetryCount(0);
//        localEventOutboxMapper.insert(outbox);

        // 4. 尝试直接发送 MQ (Happy Path 提高实时性)
        try {
            rabbitMqHelper.sendMessageWithConfirm(MQConstants.ASYNC_ORDER_EXCHANGE,
                    MQConstants.ASYNC_ORDER_KEY,
                    msg, MQConstants.MAX_RETRY_TIMES);

            // 5. 【发送成功】立刻从 Redis / MySQL 消息表中删掉它，代表消费完成
            stringRedisTemplate.opsForHash().delete(outboxKey, String.valueOf(orderId));
            //localEventOutboxMapper.deleteByOrderIdAndEventType(orderId, ORDER_CREATED_EVENT);
            log.info("订单已实时发往 MQ，并清理 Redis 暂存表和 MySQL outbox，订单号: {}", orderId);

        } catch (Exception e) {
            // 如果这里发 MQ 抛异常，或者 JVM 在第 4 步之前宕机，
            // 我们根本不慌，因为消息已经安全地躺在 Redis 的 trade:local_msg_outbox 和 MySQL outbox 里了！
            log.warn("发送 MQ 失败，转入后台定时任务补偿重试，订单号: {}", orderId, e);
        }
        return orderId;*/
    }
    // 1. 在外面（无事务）先做好所有的网络准备工作、组装好数据
    public void preHandleOrder(Long orderId, Long userId, OrderFormDTO orderFormDTO) {
        List<OrderDetailDTO> detailDTOS = orderFormDTO.getDetails();
        Map<Long, Integer> itemNumMap = detailDTOS.stream()
                .collect(Collectors.toMap(OrderDetailDTO::getItemId, OrderDetailDTO::getNum));
        Set<Long> itemIds = itemNumMap.keySet();

        // 【优化 1】：把 Feign 慢网络 IO 移出事务，保护 MySQL 连接池
        List<ItemDTO> items = itemClient.queryItemByIds(itemIds);
        if (items == null || items.size() < itemIds.size()) {
            throw new BadRequestException("item商品不存在或缺失");
        }

        int total = 0;
        for (ItemDTO item : items) {
            total += item.getPrice() * itemNumMap.get(item.getId());
        }

        // 调用真正带事务的落库方法,使用自己的代理进行请求防止this调用导致事务失效
        orderSelfProxy.handleDbOrder(orderId, userId, orderFormDTO, items, itemNumMap, total);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void handleDbOrder(Long orderId, Long userId, OrderFormDTO orderFormDTO,
                              List<ItemDTO> items, Map<Long, Integer> itemNumMap, int total) {
        log.info("开始异步落库，orderId={}, userId={}", orderId, userId);

        // 【大厂细节 2：前置幂等性防御】
        // 防止极端情况下（如 Redis 锁失效），相同的 orderId 再次进入落库逻辑。
        // 这里查主键走的是聚簇索引，速度极快，防患于未然。
        if (this.getById(orderId) != null) {
            log.warn("异步落库检测到订单 {} 已存在，触发幂等拦截，直接放行", orderId);
            return;
        }

        // 1. 查询商品与计算总价 (此部分逻辑不变)
        List<OrderDetailDTO> detailDTOS = orderFormDTO.getDetails();
        Set<Long> itemIds = itemNumMap.keySet();
        /*Map<Long, Integer> itemNumMap = detailDTOS.stream()
                .collect(Collectors.toMap(OrderDetailDTO::getItemId, OrderDetailDTO::getNum));
        Set<Long> itemIds = itemNumMap.keySet();

        log.info("异步落库查询商品信息，orderId={}, itemIds={}", orderId, itemIds);
        List<ItemDTO> items = itemClient.queryItemByIds(itemIds);
        if (items == null || items.size() < itemIds.size()) {
            throw new BadRequestException("商品不存在");
        }
        log.info("异步落库查询商品信息完成，orderId={}, itemCount={}", orderId, items.size());

        int total = 0;
        for (ItemDTO item : items) {
            total += item.getPrice() * itemNumMap.get(item.getId());
        }*/

        // 2. 组装并保存订单 (写入本地数据库)
        Order order = new Order();
        order.setId(orderId);
        order.setTotalFee(total);
        order.setPaymentType(orderFormDTO.getPaymentType());
        order.setUserId(userId);
        order.setCreateTime(LocalDateTime.now());
        order.setStatus(OrderStatusEnum.UNPAID.getCode()); // 1-未支付

        // 【大厂细节 3：底层唯一索引兜底】
        // 即使绕过了上面的 getById，由于 orderId 是主键，这里的 save 也会抛出 DuplicateKeyException，
        // 配合我们之前在 OrderMQListener 里写的 catch(DuplicateKeyException) 逻辑，形成完美闭环。
        this.save(order);
        log.info("异步落库保存订单主表成功，orderId={}", orderId);

        // 3. 保存订单详情
        List<OrderDetail> details = buildDetails(orderId, items, itemNumMap);
        detailService.saveBatch(details);
        log.info("异步落库保存订单明细成功，orderId={}, detailCount={}", orderId, details.size());

        Map<String, Object> broadcastPayload = buildOrderCreatedEventPayload(orderId, userId, detailDTOS, itemIds);
        /*broadcastPayload.put("orderId", orderId);
        broadcastPayload.put("userId", userId);
        broadcastPayload.put("orderForm", orderFormDTO);
        broadcastPayload.put("details", detailDTOS);
        broadcastPayload.put("itemIds", detailDTOS.stream().map(OrderDetailDTO::getItemId).collect(Collectors.toList()));*/
        String payloadJson = JSON.toJSONString(broadcastPayload);

        LocalEventOutbox outbox = new LocalEventOutbox();
        outbox.setOrderId(orderId);
        outbox.setEventType(MQConstants.OUTBOX_EVENT_ORDER_BROADCAST);
        outbox.setPayload(payloadJson);
        outbox.setSource("MYSQL");
        outbox.setStatus(0);
        outbox.setRetryCount(0);
        outbox.setCreateTime(LocalDateTime.now());
        outbox.setUpdateTime(LocalDateTime.now());
        localEventOutboxMapper.insert(outbox);//本地消息表是否可以被canal替代？
        log.info("异步落库写入本地事件 outbox 成功，orderId={}, outboxId={}", orderId, outbox.getId());
        // 此时insert后，MyBatis-Plus 已经自动把 ID 回填到 outbox 对象里了！
        final Long outboxId = outbox.getId();
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                try {
                    rocketMQTemplate.syncSend(
                            MQConstants.ROCKETMQ_DB_ORDER_TOPIC,
                            MessageBuilder.withPayload(payloadJson)
                                    .setHeader(RocketMQHeaders.KEYS, String.valueOf(orderId))
                                    .build()
                    );
                    //afterCommit 执行时，原先的数据库连接可能已经被 Spring 释放或者处于特殊状态。在此处再次执行数据库写操作，它会重新发起一次独立的 Auto-Commit 事务。
                    //localEventOutboxMapper.updateStatus(outboxId, 1);
                    log.info("[建单] afterCommit 广播成功。orderId={}", orderId);
                } catch (Exception e) {
                    log.warn("[建单] afterCommit 广播失败，等待 XXL-Job 补偿。orderId={}", orderId, e);
                    return; // 发送失败则不标记完成，直接返回
                }
                // 【异步回写】：MQ 发成功后，状态回写扔进独立线程池
                // 与主线程完全解耦，即使回写失败，Canal 查到 status=0 也只是走一遍"下游查库确认→幂等跳过"
                outboxStatusExecutor.execute(() -> {
                    try {
                        localEventOutboxMapper.updateStatus(outboxId, 1);
                        log.debug("[建单] outbox 状态回写成功。outboxId={}", outboxId);
                    } catch (Exception ex) {
                        // 回写失败只打日志，Canal/XXL-Job 补偿时会查库发现 MQ 其实已发，幂等跳过
                        log.warn("[建单] outbox 状态回写失败，Canal/XXL-Job 将幂等跳过。outboxId={}", outboxId, ex);
                    }
                });
            }
        });
        /*// ================== 【大厂绝杀重构】 ==================
        // 3. 彻底删除 itemClient 和 cartClient 的调用！
        // 4. 将需要通知下游的消息，组装成 JSON，写入本地消息表
        Map<String, Object> eventPayload = buildOrderCreatedEventPayload(orderId, userId, detailDTOS, itemIds);

        LocalEventOutbox outbox = new LocalEventOutbox();
        outbox.setOrderId(orderId);
        outbox.setEventType(RedisConstants.ORDER_CREATED_EVENT);
        outbox.setPayload(JSON.toJSONString(eventPayload));
        outbox.setSource("MYSQL");
        outbox.setStatus(0); // 待发送
        outbox.setRetryCount(0);
        //优化：不要在下单主链路中同步写消息表，而是改为通过 Canal 监听 MySQL Binlog 的方式，异步进行‘库存 vs 订单’的离线或近实时对账
        localEventOutboxMapper.insert(outbox); // 【核心】和保存订单在同一个事务里！
        log.info("异步落库写入本地消息表成功，orderId={}, outboxId={}", orderId, outbox.getId());

        // 5. 使用 afterCommit 作为“实时发送”的优化手段（尽最大努力）
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                try {
                    // 事务提交后，立刻发送给 RabbitMQ 的 Fanout Exchange (广播),购物车和item商品服务应该有相应的监听广播的listener
                    rabbitMqHelper.sendMessageWithConfirm(
                            MQConstants.ORDER_EVENT_EXCHANGE,
                            MQConstants.ORDER_ITEM_DEDUCT_KEY,
                            eventPayload,
                            3);

                    // 通知购物车清理已下单商品
                    rabbitMqHelper.sendMessageWithConfirm(
                            MQConstants.ORDER_CART_CLEAR_EXCHANGE,
                            MQConstants.ORDER_CART_CLEAR_KEY,
                            eventPayload,
                            3);
                    // 给自己发送延迟消息。确认订单是否已支付
                    rabbitTemplate.convertAndSend(
                            MQConstants.DELAY_EXCHANGE_NAME,
                            MQConstants.DELAY_ORDER_KEY,
                            orderId,
                            message -> {
                                message.getMessageProperties().setDelay(15 * 60 * 1000); // 15分钟
                                return message;
                            }
                    );
                    log.info("订单 {} 事务提交成功，已发出15分钟延迟关单检测消息", orderId);
                    // 发送成功，把 outbox 状态改为 1 (异步去改即可)
                    localEventOutboxMapper.updateStatus(outbox.getId(), 1);
                    
                } catch (Exception e) {
                    // 如果这里发 MQ 失败或者宕机了，完全不用慌！
                    // 因为 outbox 表里状态还是 0。
                    log.warn("实时派发订单创建事件失败，将由定时任务补偿", e);
                }
            }
        });*/
    }

    @Override
    public void markOrderPaySuccess(Long orderId) {
        // 利用 MyBatis-Plus 的 lambdaUpdate() 构建带有前置状态校验的 SQL
        boolean updated = lambdaUpdate()
                .set(Order::getStatus, OrderStatusEnum.PAID.getCode()) // 建议写成 OrderStatusEnum.PAID.getCode()
                .set(Order::getPayTime, LocalDateTime.now())
                .eq(Order::getId, orderId)
                .eq(Order::getStatus, OrderStatusEnum.UNPAID.getCode())  // 【绝对核心】前置状态必须是未支付 (1)
                .update();

        if (!updated) {
            // 如果 updated 为 false，说明底层 SQL 返回受影响的行数为 0。
            // 这意味着：要么订单不存在，要么状态已经不是 1（被别的线程抢先修改了）。
            // 在高并发支付回调场景下，这是正常的重复请求拦截，只需记录日志即可。
            log.warn("订单支付状态更新被拦截，订单可能已处理或状态异常。订单号: {}", orderId);
        } else {
            log.info("订单支付状态成功更新为已支付。订单号: {}", orderId);
        }
    }

    @Override
    // 【大厂细节 1：剔除 Seata，改用本地事务 + MQ 重试】
    // 高并发下坚决不用 @GlobalTransactional，锁资源太重。我们用本地事务保障本服务数据，跨服务依靠抛出异常让外层 RabbitMQ 重试。
    @Transactional(rollbackFor = Exception.class)
    public void cancelOrder(Long orderId) {
        // 1. 强幂等性 SQL：基于严格的状态机更新订单状态
        boolean isUpdated = lambdaUpdate()
                .set(Order::getStatus, OrderStatusEnum.CLOSED.getCode()) // 5: 已关闭
                .set(Order::getCloseTime, LocalDateTime.now())
                .eq(Order::getId, orderId)
                // 【大厂细节 2：极其关键的严谨状态判断】
                // 绝对不能用 .ne(Order::getStatus, 5)！如果订单已经支付(status=2)，你把它关了怎么办？
                // 只有未支付(status=1)的订单，才有资格被超时关闭！
                .eq(Order::getStatus, OrderStatusEnum.UNPAID.getCode())
                .update();

        // 2. 幂等性防御拦截 (拦截重复请求与并发冲突)
        if (!isUpdated) {
            // 【大厂细节 3：堵住“刷库存”漏洞】
            // 如果 isUpdated 为 false，说明订单状态不是 1（可能已经被支付，或者已经被上一次重试关单了）。
            // 此时必须直接 return！如果继续往下走，就会导致同一笔订单被重复加库存，造成商家资产严重流失！
            log.warn("延迟关单被拦截：订单状态已被改变(已支付或已关闭)，无需重复关单。订单号: {}", orderId);
            return;
        }

        // 3. 查询需要恢复的库存明细
        LambdaQueryWrapper<OrderDetail> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(OrderDetail::getOrderId, orderId);
        List<OrderDetail> orderDetails = detailService.list(queryWrapper);

        if (orderDetails == null || orderDetails.isEmpty()) {
            return;
        }

        List<OrderDetailDTO> orderDetailDTOS = orderDetails.stream()
                .map(detail -> BeanUtils.copyProperties(detail, OrderDetailDTO.class))
                .collect(Collectors.toList());

        // 4. 执行库存恢复逻辑，并保障最终一致性
        try {
            itemClient.increaseStock(orderId,orderDetailDTOS);//幂等增加库存
            log.info("订单 {} 超时未支付已成功关闭，并请求恢复商品库存成功", orderId);
        } catch (Exception e) {
            // 【大厂细节 4：异常转换与事务回滚机制】
            // 如果 Feign 调用超时或商品服务宕机，必须抛出 RuntimeException。
            // 这样不仅能触发当前方法的 @Transactional 回滚（订单状态重新变回 1），
            // 还能让外层的 OrderDelayMessageListener 捕获到异常，触发 RabbitMQ 的 NACK 机制重试。
            log.error("订单 {} 关闭时恢复下游库存失败，触发本地事务回滚，等待 MQ 重新投递", orderId, e);
            throw new RuntimeException("调用商品服务恢复库存失败", e);
        }
    }

//    /**
//     * 取消订单本地事务：只更新 MySQL 订单状态。
//     * 不发任何 MQ，不操作 Redis。
//     * MQ 通知由 RocketMQ 事务消息的 COMMIT 后由消费者负责。
//     */
//    @Override
//    @Transactional(rollbackFor = Exception.class)
//    public boolean updateStatusToCancelled(Long orderId) {
//        return lambdaUpdate()
//                .set(Order::getStatus, OrderStatusEnum.CANCELLED.getCode())
//                .set(Order::getCloseTime, LocalDateTime.now())
//                .eq(Order::getId, orderId)
//                .in(Order::getStatus, Arrays.asList(
//                        OrderStatusEnum.UNPAID.getCode(),
//                        OrderStatusEnum.PAID.getCode()))
//                .update();
//    }

    /**
     * 关单 + 库存恢复（双写一致性：outbox 模式）
     *
     * 替代原 cancelOrderAndRestore 的事务消息方案，改为：
     *   Step 1：乐观锁 UPDATE order status（幂等：仅 UNPAID 可流转）
     *   Step 2：同事务 INSERT local_event_outbox（CANCEL_RESTORE_STOCK）
     *   Step 3：afterCommit 快速路径直接 syncSend
     *           失败时 Canal 监听 outbox INSERT → 补偿发送
     *
     * 消费端（item-service CancelOrderRocketMQConsumer）逻辑完全不变。
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void cancelOrderWithOutbox(Long orderId, List<OrderDetailDTO> details) {

        // Step 1: 乐观锁关单（WHERE status = UNPAID），保证幂等
        boolean updated = lambdaUpdate()
                .eq(Order::getId, orderId)
                .eq(Order::getStatus, OrderStatusEnum.UNPAID.getCode())
                .set(Order::getStatus, OrderStatusEnum.CANCELLED.getCode())
                .set(Order::getCloseTime, LocalDateTime.now())
                .set(Order::getUpdateTime, LocalDateTime.now())
                .update();

        if (!updated) {
            // affectedRows=0：订单已支付 / 已关单 / 不存在，幂等退出
            log.info("[取消] 订单状态流转失败，安全幂等退出。orderId={}", orderId);
            return;
        }

        // Step 2: 同事务写 outbox（Canal 补偿依赖此记录）
        JSONObject payloadJson = new JSONObject();
        payloadJson.put("orderId", orderId);
        payloadJson.put("details", details);
        String payloadStr = payloadJson.toJSONString();

        LocalEventOutbox outbox = new LocalEventOutbox();
        outbox.setOrderId(orderId);
        outbox.setEventType(MQConstants.OUTBOX_EVENT_CANCEL_RESTORE);
        outbox.setPayload(payloadStr);
        outbox.setStatus(0);
        outbox.setCreateTime(LocalDateTime.now());
        outbox.setUpdateTime(LocalDateTime.now());
        localEventOutboxMapper.insert(outbox);
        // MybatisPlus insert 后 id 回填
        long capturedOutboxId = outbox.getId();

        // Step 3: 事务提交后尝试快速路径发送（与 handleDbOrder 的 afterCommit 模式完全对称）
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                try {
                    rocketMQTemplate.syncSend(
                            MQConstants.ROCKETMQ_CANCEL_TOPIC,
                            MessageBuilder.withPayload(payloadStr)
                                    .setHeader(RocketMQHeaders.KEYS, String.valueOf(orderId))
                                    .build()
                    );
                    //localEventOutboxMapper.updateStatus(capturedOutboxId, 1);
                    log.info("[取消] afterCommit 发送成功，库存恢复消息已投递。orderId={}", orderId);
                } catch (Exception e) {
                    // 快速路径失败：outbox status=0 保持不变
                    // Canal 监听到 INSERT 事件后自动补偿，无需人工介入
                    log.warn("[取消] afterCommit 发送失败，等待 Canal 补偿。orderId={}", orderId, e);
                    return;
                }
                // 【异步回写】：MQ 发成功后，状态回写扔进独立线程池
                // 与主线程完全解耦，即使回写失败，Canal 查到 status=0 也只是走一遍"查库确认→跳过"
                outboxStatusExecutor.execute(() -> {
                    try {
                        localEventOutboxMapper.updateStatus(capturedOutboxId, 1);
                        log.debug("[建单] outbox 状态回写成功。outboxId={}", capturedOutboxId);
                    } catch (Exception ex) {
                        // 回写失败只打日志，Canal/XXL-Job 补偿时会查库发现 MQ 其实已发，幂等跳过
                        log.warn("[建单] outbox 状态回写失败，Canal/XXL-Job 将幂等跳过。outboxId={}", capturedOutboxId, ex);
                    }
                });
            }
        });

        log.info("[取消] 订单已关闭，outbox 已写入。orderId={}", orderId);
    }

//    @Override
//    @Transactional(rollbackFor = Exception.class)
//    public void cancelOrderAndRestore(Long orderId, List<OrderDetailDTO> details) {
//        // ==========================================
//        // 1. MySQL 侧：幂等校验与订单状态更新
//        // ==========================================
//        Order order = getById(orderId);
//        if (order == null) {
//            log.warn("订单 {} 不存在，无需执行逆向回滚", orderId);
//            return;
//        }
//
//        if (order.getStatus() == OrderStatusEnum.CANCELLED.getCode() || order.getStatus() == OrderStatusEnum.CLOSED.getCode()) {
//            log.warn("订单 {} 已经是取消/关闭状态，MySQL 侧触发幂等拦截", orderId);
//            return;
//        }
//
//        // 如下是使用本地消息表进行取消订单并且补库存。乐观锁更新：保证状态流转的安全
//        boolean updated = lambdaUpdate()
//                .set(Order::getStatus, OrderStatusEnum.CANCELLED.getCode())
//                .set(Order::getCloseTime, LocalDateTime.now())
//                .eq(Order::getId, orderId)
//                .in(Order::getStatus, Arrays.asList(OrderStatusEnum.UNPAID.getCode(), OrderStatusEnum.PAID.getCode()))
//                .update();
//
//        if (!updated) {
//            log.error("订单 {} 状态流转异常，取消失败", orderId);
//            throw new BizIllegalException("订单取消失败，状态已被其他事务修改");
//        }
//
//        // ==========================================
//        // 2. MySQL 侧：将“退还 Redis 库存”的任务写入发件箱
//        // ==========================================
//        if (CollectionUtils.isEmpty(details)) {
//            log.error("订单详情列表details为空，状态流转异常，取消失败");
//            throw new BizIllegalException("订单取消失败，订单详情列表details为空");
//        }
//
//        // 将退库需要的上下文打包
//        Map<String, Object> payloadMap = new HashMap<>();
//        payloadMap.put("orderId", orderId);
//        payloadMap.put("details", details);
//        String payloadJson = JSON.toJSONString(payloadMap);
//
//        /*// 任务 1：恢复 Redis 预扣库存---不应该用本地消息表进行mysql与redis的缓存一致性，而应该用canal（无侵入）
//        LocalEventOutbox outbox = new LocalEventOutbox();
//        // 定义专门的事件类型，方便定时任务区分
//        outbox.setEventType("RESTORE_REDIS_STOCK");
//        outbox.setPayload(payloadJson);
//        outbox.setStatus(0); // 0-待处理*/
//
//        // 任务 2：本地消息表-》xxlJob通知 Item 服务恢复 MySQL 数据库库存
//        LocalEventOutbox itemOutbox = new LocalEventOutbox();
//        itemOutbox.setOrderId(orderId);
//        itemOutbox.setEventType("RESTORE_ITEM_STOCK");
//        itemOutbox.setPayload(payloadJson);
//        itemOutbox.setSource("MYSQL");
//        itemOutbox.setStatus(0);// 0-待处理
//        itemOutbox.setRetryCount(0);
//        itemOutbox.setCreateTime(LocalDateTime.now());
//        itemOutbox.setUpdateTime(LocalDateTime.now());
//        localEventOutboxMapper.insert(itemOutbox);
//
//        log.info("订单 {} 取消成功，已通知Item 数据库，退库任务(increase)已落库", orderId);
//        // 此时insert后，MyBatis-Plus 已经自动把 ID 回填到 outbox 对象里了！
//        final Long outboxId = itemOutbox.getId();
//        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
//            @Override
//            public void afterCommit() {
//                try {
//                    rocketMQTemplate.syncSend(
//                            MQConstants.ROCKETMQ_CANCEL_TOPIC,
//                            MessageBuilder.withPayload(payloadJson)
//                                    .setHeader(RocketMQHeaders.KEYS, String.valueOf(orderId))
//                                    .build()
//                    );
//                    localEventOutboxMapper.updateStatus(outboxId, 1);
//                    log.info("[恢复库存消息] afterCommit 发送成功。orderId={}", orderId);
//                } catch (Exception e) {
//                    log.warn("[恢复库存消息] afterCommit 发送失败，等待 XXL-Job 补偿。orderId={}", orderId, e);
//                }
//            }
//        });
//        // 组装消息体（通知 item 恢复 MySQL 库存）
//        Map<String, Object> payload = new HashMap<>();
//        payload.put("orderId", orderId);
//        payload.put("details", details);
//
//        CancelExecutionContext ctx = CancelExecutionContext.builder()
//                .orderId(orderId)
//                .details(details)
//                .build();
//
//        // 构建 RocketMQ 事务消息
//        org.springframework.messaging.Message<String> rocketMsg =
//                MessageBuilder.withPayload(JSON.toJSONString(payload))
//                        .setHeader("cancel_order_id", String.valueOf(orderId))
//                        .setHeader("cancel_create_time", String.valueOf(System.currentTimeMillis()))
//                        .build();
//
//        // 发送半事务消息（本地事务在 CancelOrderTransactionListener 中执行）
//        TransactionSendResult sendResult = cancelRocketMQTemplate.sendMessageInTransaction(
//                MQConstants.ROCKETMQ_CANCEL_TOPIC,
//                rocketMsg,
//                ctx
//        );
//
//        if (!LocalTransactionState.COMMIT_MESSAGE.equals(sendResult.getLocalTransactionState())) {
//            log.error("订单 {} 取消事务消息 ROLLBACK，取消失败", orderId);
//            throw new BizIllegalException("订单取消失败，请重试");
//        }
//
//        log.info("订单 {} 取消事务消息已 COMMIT", orderId);
//    }

    private Map<String, Object> buildOrderCreatedEventPayload(Long orderId, Long userId,
                                                               List<OrderDetailDTO> detailDTOS,
                                                               Set<Long> itemIds) {
        Map<String, Object> eventPayload = new HashMap<>();
        eventPayload.put("orderId", orderId);
        eventPayload.put("userId", userId);
        eventPayload.put("details", detailDTOS == null ? Collections.emptyList() : detailDTOS);
        eventPayload.put("itemIds", itemIds == null ? Collections.emptyList() : new ArrayList<>(itemIds));
        return eventPayload;
    }

    private List<OrderDetail> buildDetails(Long orderId, List<ItemDTO> items, Map<Long, Integer> numMap) {
        // 【大厂细节1：前置判空防御】避免引发后续的 NPE，同时也省去了不必要的 Stream 创建开销
        if (CollectionUtils.isEmpty(items)) {
            return Collections.emptyList();
        }
        return items.stream()
                .map(item -> {
                    OrderDetail detail = new OrderDetail();
                    detail.setName(item.getName());
                    detail.setSpec(item.getSpec());
                    detail.setPrice(item.getPrice());

                    // 原代码 numMap.get(item.getId()) 存在隐患，如果 Map 中恰好没有这个 itemId，
                    // 会返回 null，如果后续有拆箱操作或者对 num 进行数学运算，会直接报 NullPointerException。
                    // 优化为 getOrDefault，给一个兜底的默认值 0 (或者抛出业务异常)。
                    detail.setNum(numMap.getOrDefault(item.getId(), 0));
                    detail.setItemId(item.getId());
                    detail.setImage(item.getImage());
                    detail.setOrderId(orderId);
                    return detail;
                }).collect(Collectors.toList());
    }

}
