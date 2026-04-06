package com.hmall.trade.service.impl;

import com.alibaba.fastjson.JSON;
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
import com.hmall.trade.domain.dto.OrderFormDTO;
import com.hmall.trade.domain.enmu.OrderStatusEnum;
import com.hmall.trade.domain.po.LocalEventOutbox;
import com.hmall.trade.domain.po.Order;
import com.hmall.trade.domain.po.OrderDetail;
import com.hmall.trade.mapper.LocalEventOutboxMapper;
import com.hmall.trade.mapper.OrderMapper;
import com.hmall.trade.service.IOrderDetailService;
import com.hmall.trade.service.IOrderService;
import io.seata.spring.annotation.GlobalTransactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.LocalDateTime;
import java.util.*;
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
    private final CartClient cartClient;
    private final RabbitTemplate rabbitTemplate;
    private final RabbitMqHelper rabbitMqHelper;
    private final StringRedisTemplate stringRedisTemplate;
    private final LocalEventOutboxMapper localEventOutboxMapper;
    public static final String ITEM_DETAIL_KEY_PREFIX = "item:detail:";
    // 1. 初始化 Lua 脚本
    private static final DefaultRedisScript<Long> DEDUCT_STOCK_AND_SAVE_MSG_SCRIPT;
    static {
        DEDUCT_STOCK_AND_SAVE_MSG_SCRIPT = new DefaultRedisScript<>();
        DEDUCT_STOCK_AND_SAVE_MSG_SCRIPT.setLocation(new ClassPathResource("lua/deduct_stock.lua"));
        DEDUCT_STOCK_AND_SAVE_MSG_SCRIPT.setResultType(Long.class);
    }
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

    //private final ICartService cartService;
//    @Override
//    // 注意：此处不再加 @GlobalTransactional，因为是异步化处理
//    public Long createOrder(OrderFormDTO orderFormDTO) {
//        // ================= 【新代码：多商品并发预扣减】 =================
//        // 1. 获取本次下单的所有商品明细
//        List<OrderDetailDTO> details = orderFormDTO.getDetails();
//        // 2. 收集所有的 RLock，准备组装联锁
//        List<RLock> locks = new ArrayList<>();
//        // 为了防止死锁的极致稳妥做法：先对商品ID进行排序 (虽然MultiLock有机制，但业务层排序最安全)
//        details.sort(Comparator.comparing(OrderDetailDTO::getItemId));
//        for (OrderDetailDTO detail : details) {
//            String lockKey = "lock:item:" + detail.getItemId();
//            locks.add(redissonClient.getLock(lockKey));
//        }
//        // 3. 将 List 转换为数组，并创建 Redisson 联锁 (MultiLock)
//        RLock multiLock = redissonClient.getMultiLock(locks.toArray(new RLock[0]));
//        try {
//            // 4. 尝试获取联锁，等待10秒。获取成功后，所有相关的商品都被锁住了！看门狗同样生效。
//            if (multiLock.tryLock(10, TimeUnit.SECONDS)) {
//                // 用于记录已经扣减成功的商品，方便一旦发生异常时进行回滚补偿
//                List<OrderDetailDTO> deductedItems = new ArrayList<>();
//                try {
//                    // 5. 遍历扣减 Redis 库存
//                    for (OrderDetailDTO detail : details) {
//                        Long itemId = detail.getItemId();
//                        Integer num = detail.getNum();
//                        String stockKey = "item:stock:" + itemId;
//                        // 执行原子扣减
//                        Long remainStock = redissonClient.getAtomicLong(stockKey).addAndGet(-num);
//                        if (remainStock < 0) {
//                            // 扣减失败，立刻把当前加为负数的加回来
//                            redissonClient.getAtomicLong(stockKey).addAndGet(num);
//                            // 抛出异常，跳到 catch 块执行全体补偿
//                            throw new BizIllegalException("商品 " + itemId + " 库存不足！");
//                        }
//                        // 记录扣减成功的商品
//                        deductedItems.add(detail);
//                    }
//                    // 6. 所有商品预扣减全部成功！提前生成订单号
//                    Long orderId = IdWorker.getId();
//                    // 7. 封装消息发送到 MQ (这部分逻辑不变)
//                    Map<String, Object> msg = new HashMap<>();
//                    msg.put("orderId", orderId);
//                    msg.put("userId", UserContext.getUser());
//                    msg.put("orderForm", orderFormDTO);
////                    rabbitTemplate.convertAndSend(
////                            MQConstants.ASYNC_ORDER_EXCHANGE,
////                            MQConstants.ASYNC_ORDER_KEY,
////                            msg
////                    );
//                    //设置最大重试次数
//                    int maxRetries=3;
//                    //利用common封装的mq重试机制发送消息
//                    rabbitMqHelper.sendMessageWithConfirm(MQConstants.ASYNC_ORDER_EXCHANGE,
//                            MQConstants.ASYNC_ORDER_KEY,
//                            msg,maxRetries);
//                    log.info("多商品订单已提交异步处理，订单号: {}", orderId);
//                    return orderId;
//                } catch (BizIllegalException e) {
//                    // 💥【核心补偿逻辑】如果中间某个商品库存不足，必须把前面扣减成功的全加回来！
//                    log.warn("扣减过程发生库存不足，执行回滚补偿...");
//                    for (OrderDetailDTO deducted : deductedItems) {
//                        String stockKey = "item:stock:" + deducted.getItemId();
//                        redissonClient.getAtomicLong(stockKey).addAndGet(deducted.getNum());
//                    }
//                    throw e; // 继续向上抛出，告诉前端库存不足
//                }
//            }
//        } catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//            throw new BizIllegalException("系统繁忙，请稍后再试");
//        } finally {
//            // 8. 释放联锁 (底层会自动释放所有相关的子锁)
//            // 注意：因为涉及到多个锁，只要当前线程持有其中任何一个锁，都应该尝试释放
//            try {
//                multiLock.unlock();
//            } catch (Exception e) {
//                log.warn("释放联锁发生异常（可能是部分锁已过期），忽略", e);
//            }
//        }
//        throw new BizIllegalException("获取商品资源失败，请重试");
//    }
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
        Object[] args = new Object[orderFormDTO.getDetails().size() + 2]; // 多加 2 个位置放 orderId 和 payload

        int i = 0;
        for (; i < orderFormDTO.getDetails().size(); i++) {
            OrderDetailDTO detail = orderFormDTO.getDetails().get(i);
            keys.add( ITEM_DETAIL_KEY_PREFIX+ detail.getItemId());
            args[i] = String.valueOf(detail.getNum());
        }
        // 【核心新增】指定 Redis 消息表的 Key，并将 payload 传入；如果担心所有order都存一个key的大key问题，可以在 Redis 里约定 16 个 Hash 表
        //trade:local_msg_outbox:0~trade:local_msg_outbox:15，然后写入时:把 orderId % 16，补偿时:定时任务用多线程，分别去捞这 16 个桶即可
        keys.add("trade:local_msg_outbox");
        args[i] = String.valueOf(orderId);
        args[i + 1] = msgJson;

        // 3. 执行 Lua 脚本，原子性扣减库存 + 保存消息！
        // 此时就算下一秒机器突然断电炸了，只要 Lua 执行成功，Redis 里必定同时有扣减后的库存和这条待发送的消息。
        Long result = stringRedisTemplate.execute(DEDUCT_STOCK_AND_SAVE_MSG_SCRIPT, keys, args);

        if (result != null && !result.equals(LuaConstants.LUA_SUCCESS)) {
            log.warn("用户 {} 下单失败，商品 {} 库存不足", userId, result);
            throw new BizIllegalException("部分商品库存不足，下单失败！");
        }

        // 4. 尝试直接发送 MQ (Happy Path 提高实时性)
        try {
            rabbitMqHelper.sendMessageWithConfirm(MQConstants.ASYNC_ORDER_EXCHANGE,
                    MQConstants.ASYNC_ORDER_KEY,
                    msg, MQConstants.MAX_RETRY_TIMES);

            // 5. 【发送成功】立刻从 Redis 消息表中删掉它，代表消费完成
            stringRedisTemplate.opsForHash().delete("trade:local_msg_outbox", String.valueOf(orderId));
            log.info("订单已实时发往 MQ，并清理 Redis 暂存表，订单号: {}", orderId);

        } catch (Exception e) {
            // 如果这里发 MQ 抛异常，或者 JVM 在第 4 步之前宕机，
            // 我们根本不慌，因为消息已经安全地躺在 Redis 的 trade:local_msg_outbox 里了！
            log.warn("发送 MQ 失败，转入后台定时任务补偿重试，订单号: {}", orderId, e);
        }
        return orderId;
    }

    @Override
    // 【大厂细节 1：剔除 Seata】摒弃沉重的分布式事务，改用普通的本地事务。跨服务的一致性交由 MQ 重试来保障。
    @Transactional(rollbackFor = Exception.class)
    public void handleDbOrder(Long orderId, Long userId, OrderFormDTO orderFormDTO) {

        // 【大厂细节 2：前置幂等性防御】
        // 防止极端情况下（如 Redis 锁失效），相同的 orderId 再次进入落库逻辑。
        // 这里查主键走的是聚簇索引，速度极快，防患于未然。
        if (this.getById(orderId) != null) {
            log.warn("异步落库检测到订单 {} 已存在，触发幂等拦截，直接放行", orderId);
            return;
        }

        // 1. 查询商品与计算总价 (此部分逻辑不变)
        List<OrderDetailDTO> detailDTOS = orderFormDTO.getDetails();
        Map<Long, Integer> itemNumMap = detailDTOS.stream()
                .collect(Collectors.toMap(OrderDetailDTO::getItemId, OrderDetailDTO::getNum));
        Set<Long> itemIds = itemNumMap.keySet();

        List<ItemDTO> items = itemClient.queryItemByIds(itemIds);
        if (items == null || items.size() < itemIds.size()) {
            throw new BadRequestException("商品不存在");
        }

        int total = 0;
        for (ItemDTO item : items) {
            total += item.getPrice() * itemNumMap.get(item.getId());
        }

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

        // 3. 保存订单详情
        List<OrderDetail> details = buildDetails(orderId, items, itemNumMap);
        detailService.saveBatch(details);
        // ================== 【大厂绝杀重构】 ==================
        // 3. 彻底删除 itemClient 和 cartClient 的调用！
        // 4. 将需要通知下游的消息，组装成 JSON，写入本地消息表
        Map<String, Object> eventPayload = new HashMap<>();
        eventPayload.put("orderId", orderId);
        eventPayload.put("details", detailDTOS);
        eventPayload.put("itemIds", itemIds);

        LocalEventOutbox outbox = new LocalEventOutbox();
        outbox.setEventType("ORDER_CREATED");
        outbox.setPayload(JSON.toJSONString(eventPayload));
        outbox.setStatus(0); // 待发送
        localEventOutboxMapper.insert(outbox); // 【核心】和保存订单在同一个事务里！

        // 5. 使用 afterCommit 作为“实时发送”的优化手段（尽最大努力）
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                try {
                    // 事务提交后，立刻发送给 RabbitMQ 的 Fanout Exchange (广播),购物车和item商品服务应该有相应的监听广播的listener但我没创建，省事
                    rabbitMqHelper.sendMessageWithConfirm(
                            "trade.topic.exchange", "order.created", eventPayload, 3);

                    // 发送成功，把 outbox 状态改为 1 (异步去改即可)
                    localEventOutboxMapper.updateStatus(outbox.getId(), 1);
                } catch (Exception e) {
                    // 如果这里发 MQ 失败或者宕机了，完全不用慌！
                    // 因为 outbox 表里状态还是 0。
                    log.warn("实时派发订单创建事件失败，将由定时任务补偿", e);
                }
            }
        });
    }
    /*采用client客户端远程RPC调用，存在大问题
        @Override
        // 【大厂细节 1：剔除 Seata】摒弃沉重的分布式事务，改用普通的本地事务。跨服务的一致性交由 MQ 重试来保障。
        @Transactional(rollbackFor = Exception.class)
        public void handleDbOrder(Long orderId, Long userId, OrderFormDTO orderFormDTO) {

            // 【大厂细节 2：前置幂等性防御】
            // 防止极端情况下（如 Redis 锁失效），相同的 orderId 再次进入落库逻辑。
            // 这里查主键走的是聚簇索引，速度极快，防患于未然。
            if (this.getById(orderId) != null) {
                log.warn("异步落库检测到订单 {} 已存在，触发幂等拦截，直接放行", orderId);
                return;
            }

            // 1. 查询商品与计算总价 (此部分逻辑不变)
            List<OrderDetailDTO> detailDTOS = orderFormDTO.getDetails();
            Map<Long, Integer> itemNumMap = detailDTOS.stream()
                    .collect(Collectors.toMap(OrderDetailDTO::getItemId, OrderDetailDTO::getNum));
            Set<Long> itemIds = itemNumMap.keySet();

            List<ItemDTO> items = itemClient.queryItemByIds(itemIds);
            if (items == null || items.size() < itemIds.size()) {
                throw new BadRequestException("商品不存在");
            }

            int total = 0;
            for (ItemDTO item : items) {
                total += item.getPrice() * itemNumMap.get(item.getId());
            }

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

            // 3. 保存订单详情
            List<OrderDetail> details = buildDetails(orderId, items, itemNumMap);
            detailService.saveBatch(details);

            // 4. 清理购物车商品 (跨服务 RPC)
            // 【大厂细节 4：核心链路降级】清理购物车属于“非核心弱依赖”逻辑。
            // 如果购物车服务挂了，不能影响用户下单。所以必须 try-catch 吞掉异常，只打日志，不回滚。
            try {
                cartClient.deleteCartItemByIds(itemIds);
            } catch (Exception e) {
                log.error("订单 {} 清理购物车失败，已降级忽略此异常，保障主链路顺畅", orderId, e);
            }

            // 5. 扣减底层数据库真实库存 (跨服务 RPC)
            // 【大厂细节 5：最终一致性的核心难点】
            try {
                // 注意：在大厂设计中，这里的 deductStock 参数里 必须把 orderId 也传过去！
                 itemClient.deductStock(orderId, detailDTOS);
    //            itemClient.deductStock(detailDTOS);
            } catch (Exception e) {
                // 如果扣减库存超时失败，抛出运行时异常，触发当前 @Transactional 事务回滚（订单不落库）。
                // 异常抛出后，外层的 RabbitMQ 监听器会执行 basicNack，让消息重试。
                log.error("订单 {} 扣减底层库存失败，触发本地落库回滚，等待 MQ 重新投递", orderId, e);
                throw new RuntimeException("底层库存扣减失败", e);
            }

            // 6. 发送延迟消息，检测订单支付状态
            // 【大厂顶级细节 6：事务同步管理器】
            // 绝不能直接发 MQ！假设直接发送了延迟消息，但随后数据库 commit 失败（比如网络波动导致连接断开），
            // 延迟消息依然会被发送出去。15分钟后去查一个根本不存在的订单，毫无意义甚至可能报错。
            // 解决方法：注册事务同步器，只有在当前本地数据库事务 【明确 Commit 成功后】，才向 RabbitMQ 投递消息！
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {//在 afterCommit 中，如果不幸抛出了 Redis 连接异常，可以借助 Spring Retry 提供本地重试 3 次。
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
                }
            });
            //如果重试 3 次还是失败，系统只记录一条 ERROR 日志，或者把这条失败记录写进一张 MySQL 的 cache_error_log 异常表。
            //到了夜深人静（或者每隔 5 分钟），系统会有一个 XXL-JOB 定时任务跑起来，把异常表里的商品查出来，对比一遍 MySQL 和 Redis，如果不对，就以 MySQL 为准覆盖 Redis。
        }
    */
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

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void cancelOrderAndRestore(Long orderId, List<OrderDetailDTO> details) {
        // ==========================================
        // 1. MySQL 侧：幂等校验与订单状态更新
        // ==========================================
        Order order = getById(orderId);
        if (order == null) {
            log.warn("订单 {} 不存在，无需执行逆向回滚", orderId);
            return;
        }

        if (order.getStatus() == OrderStatusEnum.CANCELLED.getCode() || order.getStatus() == OrderStatusEnum.CLOSED.getCode()) {
            log.warn("订单 {} 已经是取消/关闭状态，MySQL 侧触发幂等拦截", orderId);
            return;
        }

        // 乐观锁更新：保证状态流转的安全
        boolean updated = lambdaUpdate()
                .set(Order::getStatus, OrderStatusEnum.CANCELLED.getCode())
                .set(Order::getCloseTime, LocalDateTime.now())
                .eq(Order::getId, orderId)
                .in(Order::getStatus, Arrays.asList(OrderStatusEnum.UNPAID.getCode(), OrderStatusEnum.PAID.getCode()))
                .update();

        if (!updated) {
            log.error("订单 {} 状态流转异常，取消失败", orderId);
            throw new BizIllegalException("订单取消失败，状态已被其他事务修改");
        }

        // ==========================================
        // 2. MySQL 侧：将“退还 Redis 库存”的任务写入发件箱
        // ==========================================
        if (CollectionUtils.isNotEmpty(details)) {
            // 将退库需要的上下文打包
            Map<String, Object> payloadMap = new HashMap<>();
            payloadMap.put("orderId", orderId);
            payloadMap.put("details", details);
            String payloadJson = JSON.toJSONString(payloadMap);

            // 任务 1：恢复 Redis 预扣库存---不应该用本地消息表进行mysql与redis的缓存一致性，而应该用canal（无侵入）
            /*LocalEventOutbox outbox = new LocalEventOutbox();
            // 定义专门的事件类型，方便定时任务区分
            outbox.setEventType("RESTORE_REDIS_STOCK");
            outbox.setPayload(payloadJson);
            outbox.setStatus(0); // 0-待处理*/

            // 任务 2：本地消息表-》xxlJob通知 Item 服务恢复 MySQL 数据库库存
            LocalEventOutbox itemOutbox = new LocalEventOutbox();
            itemOutbox.setEventType("RESTORE_ITEM_STOCK");
            itemOutbox.setPayload(payloadJson);
            itemOutbox.setStatus(0);// 0-待处理
            localEventOutboxMapper.insert(itemOutbox);

            // 【重中之重】：outboxMapper的插入，和订单状态的修改，处在同一个 @Transactional 中！
//            localEventOutboxMapper.insert(outbox);
            log.info("订单 {} 取消成功，已通知Item 数据库，退库任务(increase)已落库", orderId);
        }
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
    /* 没有考虑createOrder中redis和MQ的双写一致性问题
    public Long createOrder(OrderFormDTO orderFormDTO) {
        Long userId = UserContext.getUser(); // 从 ThreadLocal 获取用户 ID

        // 1. 准备 Lua 脚本需要的 KEYS 和 ARGV
        List<String> keys = new ArrayList<>();
        Object[] args = new Object[orderFormDTO.getDetails().size()];


        for (int i = 0;; i < orderFormDTO.getDetails().size(); i++) {
            OrderDetailDTO detail = orderFormDTO.getDetails().get(i);
            keys.add("item:stock:" + detail.getItemId()); // Redis中的库存Key
            args[i] = String.valueOf(detail.getNum());    // 扣减数量
        }

        // 2. 执行 Lua 脚本，原子性扣减库存
        Long result = stringRedisTemplate.execute(DEDUCT_STOCK_SCRIPT, keys, args);

        // 3. 判断扣减结果
        if (result != null && !result.equals(LuaConstants.LUA_SUCCESS)) {
            // result 返回的是库存不足的商品 ID (需要在 Lua 脚本中约定)
            log.warn("用户 {} 下单失败，商品 {} 库存不足", userId, result);
            throw new BizIllegalException("部分商品库存不足，下单失败！"); // 抛出自定义异常给前端
        }

        // 4. 库存扣减成功，利用雪花算法生成唯一订单号
        Long orderId = IdWorker.getId();

        // 5. 封装消息发送到 MQ (这部分逻辑不变)
        Map<String, Object> msg = new HashMap<>();
        msg.put("orderId", orderId);
        msg.put("userId", UserContext.getUser());
        msg.put("orderForm", orderFormDTO);
        // ... 其他收货地址等信息

        // 6. 发送消息到 RabbitMQ，异步落库
        //利用common封装的mq重试机制发送消息
        rabbitMqHelper.sendMessageWithConfirm(MQConstants.ASYNC_ORDER_EXCHANGE,
                MQConstants.ASYNC_ORDER_KEY,
                msg,MQConstants.MAX_RETRY_TIMES);
        log.info("多商品订单已提交异步处理，订单号: {}", orderId);
        return orderId;

    }
     */
}
