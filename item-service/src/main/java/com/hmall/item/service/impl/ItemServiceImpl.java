package com.hmall.item.service.impl;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmall.api.dto.ItemDTO;
import com.hmall.api.dto.OrderDetailDTO;
import com.hmall.common.domain.PageDTO;
import com.hmall.common.domain.PageQuery;
import com.hmall.common.exception.BadRequestException;
import com.hmall.common.exception.BizIllegalException;
import com.hmall.common.utils.BeanUtils;
import com.hmall.common.utils.RabbitMqHelper;
import com.hmall.item.config.ItemCachePreloader;
import com.hmall.item.constants.MQConstants;
import com.hmall.item.domain.po.Item;
import com.hmall.item.domain.po.StockDeductLog;
import com.hmall.item.mapper.ItemMapper;
import com.hmall.item.mapper.StockDeductLogMapper;
import com.hmall.item.service.IItemService;
import com.hmall.item.service.IItemStockVersionService;
import com.hmall.item.task.BloomFilterJobHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * <p>
 * 商品表 服务实现类
 * </p>
 *
 * @author 虎哥
 */
@Slf4j
@Service
@RequiredArgsConstructor // 确保加入了注入注解
public class ItemServiceImpl extends ServiceImpl<ItemMapper, Item> implements IItemService {

    private final StringRedisTemplate stringRedisTemplate;
    private final RedissonClient redissonClient;
    // 注入流水表 Mapper
    private final StockDeductLogMapper stockDeductLogMapper;
    private final IItemStockVersionService itemStockVersionService;

    private static final String STOCK_VERSION_KEY_PREFIX = ItemCachePreloader.ITEM_STOCK_VERSION_KEY_PREFIX;

    private String stockKey(Long itemId) {
        return ItemCachePreloader.ITEM_STOCK_KEY_PREFIX + itemId;
    }

    private String stockVersionKey(Long itemId) {
        return STOCK_VERSION_KEY_PREFIX + itemId;
    }

    // 1. 定义 Lua 脚本
    /*private static final DefaultRedisScript<Long> BATCH_INCR_SCRIPT;
    static {
        BATCH_INCR_SCRIPT = new DefaultRedisScript<>();
        // Lua 脚本：遍历 KEYS 和 ARGV，原子批量增加
        BATCH_INCR_SCRIPT.setScriptText(
                "for i, key in ipairs(KEYS) do " +
                        "   local num = tonumber(ARGV[i]) " +
                        "   redis.call('incrby', key, num) " +
                        "end " +
                        "return 1"
        );
        BATCH_INCR_SCRIPT.setResultType(Long.class);
    }*/

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void deductStock(Long orderId, List<OrderDetailDTO> items) {

        // 1. 第一道防线：尝试插入扣减流水，利用数据库唯一索引实现强幂等。
        // StockDeductLog 只承担幂等状态机职责，不再承载 Redis/MySQL 的版本事实。
        StockDeductLog deductLog = new StockDeductLog();
        deductLog.setOrderId(orderId);
        deductLog.setStatus(1); // 1表示正常扣减

        try {
            stockDeductLogMapper.insert(deductLog);
        } catch (DuplicateKeyException e) {
            // 【大厂神操作】
            // 如果捕获到唯一键冲突异常，说明该 orderId 以前已经成功扣减过库存了。
            // 此时直接 return，不抛出异常，让外层（如 MQ 或 Feign）以为这次调用也成功了。
            log.warn("检测到重复扣减库存请求，触发幂等放行。订单号: {}", orderId);
            return;
        }

        // 2. 第二道防线：执行安全扣减 (修复了超卖漏洞)-----for循环导致网络io频繁效率低
//        for (OrderDetailDTO item : items) {
//            // 调用我们在 ItemMapper 中写的基于数据库乐观锁的安全 SQL
//            int affectedRows = baseMapper.deductStockSafe(item.getItemId(), item.getNum());
//            if (affectedRows == 0) {
//                // affectedRows 为 0 说明什么？说明 itemId 不存在，或者 stock < num 了！
//                // 此时抛出异常。由于加了 @Transactional，刚才插入的 StockDeductLog 会一并回滚！
//                // 这样既保证了不超卖，又保证了不会留下脏流水。
//                log.error("商品 {} 库存不足或不存在，扣减失败", item.getItemId());
//                throw new BizIllegalException("商品 " + item.getItemId() + " 库存不足！");
//            }
//        }
        //订单 1 买了【商品 A、商品 B】，MySQL 加锁顺序：先锁 A，再锁 B。
        //订单 2 买了【商品 B、商品 A】，MySQL 加锁顺序：先锁 B，再锁 A。
        //如果这两个订单几乎同时发往 MySQL，瞬间触发 Deadlock（死锁）！事务双双回滚。

        // 【优化 1：防止 MySQL 批量更新死锁】
        // 强制按 itemId 排序，保证所有线程/事务获取行级锁的顺序一致！
        items.sort(Comparator.comparing(OrderDetailDTO::getItemId));
        // 2. 批量安全扣减
        int affectedRows = baseMapper.batchDeductStockSafe(items);
        // 3. 一次性校验结果
        if (affectedRows != items.size()) {
            // 假设有 3 个商品，但 affectedRows 是 2，说明有 1 个商品触发了 stock >= num 的防线（或者压根不存在）。
            // 此时直接抛出异常，触发整个本地事务回滚（包括刚刚插入的流水表）！
            log.error("订单 {} 批量扣减库存失败，存在商品库存不足，受影响行数: {}", orderId, affectedRows);
            throw new BizIllegalException("部分商品库存不足，扣减失败！");
        }

        for (OrderDetailDTO item : items) {
            itemStockVersionService.recordStockChange(item.getItemId(), orderId, "DEDUCT");
        }

        log.info("订单 {} 库存批量扣减成功，并记录幂等流水完毕", orderId);
    }

    /**
     * 懒加载：将指定商品的 MySQL 库存写入 Redis。
     *
     * <p>触发时机：trade-service 执行 Lua 扣减时发现库存 key 缺失（Lua 返回负数 index），
     * 通过 Feign 调用此方法，写入后重试 Lua。
     *
     * <p>设计说明：
     * <ul>
     *   <li>TTL 设置为 30 分钟（短 TTL）：不影响凌晨对账接管长期同步逻辑；</li>
     *   <li>使用 {@code setIfAbsent}：防止并发懒加载时覆盖已经由正常预热写入的值；</li>
     *   <li>同步初始化 per-item version key（{@code epoch|0}），让对账能正确读取版本快照；</li>
     *   <li>若商品不存在或已下架，不写入任何 key，让 Lua 继续返回 nil，
     *       上层会保守 ROLLBACK，不会误扣库存。</li>
     * </ul>
     *
     * @param itemId 商品 ID
     */
    @Override
    public void loadStockToRedis(Long itemId) {
        // 1. 从 MySQL 查询商品（包含库存）
        Item item = getById(itemId);
        if (item == null || item.getStatus() != 1) {
            // 商品不存在或已下架，不写 Redis：
            // Lua 重试时仍拿到 nil，上层保守 ROLLBACK，安全
            log.warn("[懒加载] 商品不存在或已下架，跳过写入 Redis。itemId={}", itemId);
            return;
        }

        String stockKey   = ItemCachePreloader.ITEM_STOCK_KEY_PREFIX   + itemId;
        String versionKey = ItemCachePreloader.ITEM_STOCK_VERSION_KEY_PREFIX + itemId;

        // 2. 写入库存 key（短 TTL = 30min，凌晨对账接管后续同步）
        //    setIfAbsent：防止并发情况下覆盖已有的预热值
        Boolean stockSet = stringRedisTemplate.opsForValue()
                .setIfAbsent(stockKey, String.valueOf(item.getStock()), 30, TimeUnit.MINUTES);

        if (Boolean.TRUE.equals(stockSet)) {
            log.info("[懒加载] 库存 key 写入成功。itemId={}，stock={}", itemId, item.getStock());
        } else {
            // 并发时已有其他线程写入，直接复用，无需覆盖
            log.info("[懒加载] 库存 key 已存在（并发写入），复用。itemId={}", itemId);
        }

        // 3. 同步初始化 per-item version key（格式：epoch|seq）
        //    从 Redis 读取当前全局 epoch，写入 "epoch|0" 作为该商品的版本基准
        //    让凌晨对账能读取到合理的 redisEpoch，避免触发误报
        String globalEpoch = stringRedisTemplate.opsForValue()
                .get(ItemCachePreloader.ITEM_STOCK_EPOCH_KEY);
        String initVersion = (globalEpoch != null ? globalEpoch : "1") + "|0";
        stringRedisTemplate.opsForValue()
                .setIfAbsent(versionKey, initVersion, 30, TimeUnit.MINUTES);

        log.info("[懒加载] per-item version key 初始化完成。itemId={}，version={}", itemId, initVersion);
    }

    /**
     * 核心高并发分页查询：数据与索引分离 + DCL双重检查锁
     */
    public PageDTO<ItemDTO> queryItemByPageWithCache(PageQuery query) {
        int start = query.from();
        int size = query.getPageSize();

        // 1. 防御性设计：超限查询直接拦截或走DB
        if (start > 1000) {
            throw new BadRequestException("查询页码过大");
        }
        if (start >= 500 || StrUtil.isNotBlank(query.getSortBy())) {
            Page<Item> dbPage = this.page(query.toMpPage("update_time", false));
            return PageDTO.of(dbPage, ItemDTO.class);
        }

        String indexKey = ItemCachePreloader.ITEM_INDEX_KEY;
        int end = start + size - 1;

        // 2. 🌟 第一层安全防线：获取 ZSet 中的 ID 列表 (内部自带 DCL 防击穿)
        List<String> itemIds = getZSetIndexWithCache(indexKey, start, end);
        if (CollUtil.isEmpty(itemIds)) {
            return new PageDTO<>(0L, 0L, Collections.emptyList());
        }

        // 3. 🌟 第二层安全防线：根据 ID 获取详情 (内部自带 DCL 和 布隆防穿透)
        List<ItemDTO> resultList = getItemDetailsWithCache(itemIds);

        // 4. 组装返回
        Long total = stringRedisTemplate.opsForZSet().zCard(indexKey);
        PageDTO<ItemDTO> pageDTO = new PageDTO<>();
        pageDTO.setTotal(total != null ? total : 0L);
        pageDTO.setPages((total + size - 1) / size);
        pageDTO.setList(resultList);

        return pageDTO;
    }

    private List<String> getZSetIndexWithCache(String indexKey, int start, int end) {
        // 第一次尝试查 Redis
        Set<String> itemIds = stringRedisTemplate.opsForZSet().reverseRange(indexKey, start, end);
        if (CollUtil.isNotEmpty(itemIds)) {
            return new ArrayList<>(itemIds);
        }

        // 缓存未命中，获取互斥锁防击穿
        RLock lock = redissonClient.getLock("lock:rebuild:item:index");
        try {
            if (lock.tryLock(3, 10, TimeUnit.SECONDS)) {
                // DCL：拿到锁后再次检查
                itemIds = stringRedisTemplate.opsForZSet().reverseRange(indexKey, start, end);
                if (CollUtil.isNotEmpty(itemIds)) {
                    return new ArrayList<>(itemIds);
                }
                // 确认没了，重建前 500 条热点索引和详情
                rebuildHotItemCache();
                return new ArrayList<>(stringRedisTemplate.opsForZSet().reverseRange(indexKey, start, end));
            } else {
                // 没拿到锁，说明有人在重建，休眠重试
                Thread.sleep(50);
                return getZSetIndexWithCache(indexKey, start, end);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("系统繁忙，请稍后再试");
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    private List<ItemDTO> getItemDetailsWithCache(List<String> itemIds) {
        List<String> detailKeys = itemIds.stream()
                .map(id -> ItemCachePreloader.ITEM_DETAIL_KEY_PREFIX + id)
                .collect(Collectors.toList());

        // 1. 批量查询 Redis 详情
        List<String> jsonItems = stringRedisTemplate.opsForValue().multiGet(detailKeys);

        // 存放成功获取的商品 (由于我们要保持 ZSet 的原始顺序，所以用 Map 暂存)
        Map<String, ItemDTO> itemDtoMap = new HashMap<>();
        List<Long> missingIds = new ArrayList<>();

        for (int i = 0; i < itemIds.size(); i++) {
            String idStr = itemIds.get(i);
            String json = jsonItems.get(i);

            if (StrUtil.isNotBlank(json)) {
                if (!"{}".equals(json)) {
                    itemDtoMap.put(idStr, BeanUtils.copyBean(JSONUtil.toBean(json, Item.class), ItemDTO.class));
                }
                // 如果是 "{}" 则既不查 DB 也不放进 map，直接跳过
            } else {
                missingIds.add(Long.valueOf(idStr)); // 记录真实缺失的 ID
            }
        }

        // ==========================================
        // 🌟 漏洞修复区：对缺失的详情缓存加锁重建 🌟
        // ==========================================
        if (CollUtil.isNotEmpty(missingIds)) {
            // 使用详情专用的锁，避免和重建索引的锁冲突
            RLock detailLock = redissonClient.getLock("lock:rebuild:item:details");
            try {
                if (detailLock.tryLock(3, 10, TimeUnit.SECONDS)) {

                    // 🌟【第二层 DCL】：拿到锁后，必须再查一次 Redis！防止阻塞在获取锁阶段的线程重复查 DB
                    List<String> checkKeys = missingIds.stream()
                            .map(id -> ItemCachePreloader.ITEM_DETAIL_KEY_PREFIX + id)
                            .collect(Collectors.toList());
                    List<String> checkJsons = stringRedisTemplate.opsForValue().multiGet(checkKeys);

                    List<Long> realMissingIds = new ArrayList<>();
                    for (int i = 0; i < missingIds.size(); i++) {
                        String json = checkJsons.get(i);
                        Long id = missingIds.get(i);
                        if (StrUtil.isNotBlank(json)) {
                            if (!"{}".equals(json)) {
                                itemDtoMap.put(String.valueOf(id), BeanUtils.copyBean(JSONUtil.toBean(json, Item.class), ItemDTO.class));
                            }
                        } else {
                            realMissingIds.add(id); // 经过 DCL 确认，真的是缺失的！
                        }
                    }

                    // ==========================================
                    // 🌟 安全区域：此时只有拿到锁的 1 个线程能拿着 realMissingIds 去查 DB
                    // ==========================================
                    if (CollUtil.isNotEmpty(realMissingIds)) {
                        RBloomFilter<Long> bloomFilter = redissonClient.getBloomFilter(BloomFilterJobHandler.BLOOM_FILTER_KEY);
                        List<Long> passBloomFilterIds = new ArrayList<>();

                        for (Long id : realMissingIds) {
                            if (bloomFilter.isExists() && !bloomFilter.contains(id)) {
                                // 布隆拦截，直接写空值
                                stringRedisTemplate.opsForValue().set(ItemCachePreloader.ITEM_DETAIL_KEY_PREFIX + id, "{}", 3, TimeUnit.MINUTES);
                            } else {
                                passBloomFilterIds.add(id);
                            }
                        }

                        if (CollUtil.isNotEmpty(passBloomFilterIds)) {
                            List<Item> dbItems = this.listByIds(passBloomFilterIds);
                            Map<Long, Item> dbItemMap = dbItems.stream().collect(Collectors.toMap(Item::getId, Function.identity()));

                            for (Long id : passBloomFilterIds) {
                                Item item = dbItemMap.get(id);
                                if (item != null && item.getStatus() == 1) {
                                    // 查到了真实数据，回写 Redis
                                    itemDtoMap.put(String.valueOf(id), BeanUtils.copyBean(item, ItemDTO.class));
                                    stringRedisTemplate.opsForValue().set(
                                            ItemCachePreloader.ITEM_DETAIL_KEY_PREFIX + id,
                                            JSONUtil.toJsonStr(item),
                                            60 + RandomUtil.randomInt(0, 10), TimeUnit.MINUTES
                                    );
                                } else {
                                    // 没查到或已下架，写空值兜底
                                    stringRedisTemplate.opsForValue().set(ItemCachePreloader.ITEM_DETAIL_KEY_PREFIX + id, "{}", 3, TimeUnit.MINUTES);
                                }
                            }
                        }
                    }
                } else {
                    // 没拿到锁，说明有别的线程正在疯狂查DB重建这几个缺失的详情
                    // 休眠后重试整个获取详情的方法
                    Thread.sleep(50);
                    return getItemDetailsWithCache(itemIds);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                if (detailLock.isHeldByCurrentThread()) {
                    detailLock.unlock();
                }
            }
        }

        // 最终，按照入参 itemIds 的严格顺序，从 map 中把数据挑出来组装成 List (保证分页排序不乱)
        List<ItemDTO> finalSortedList = new ArrayList<>();
        for (String idStr : itemIds) {
            ItemDTO dto = itemDtoMap.get(idStr);
            if (dto != null) {
                finalSortedList.add(dto);
            }
        }
        return finalSortedList;
    }
//    public PageDTO<ItemDTO> queryItemByPageWithCache(PageQuery query) {
//        int start = query.from();
//        int size = query.getPageSize();
//
//        // 【防御性设计】如果查询的深度超过了我们缓存的热点数据容量（500条），或者带有复杂搜索条件
//        // 这种请求天然并发低，直接走数据库
//        // 限制最大查询页数，比如商城通常只允许用户看前 100 页（1000条）
//        if (start > 1000) {
//            throw new BadRequestException("查询页码过大"); // 直接掐断，保护DB
//        }
//        if (start >= 500 || StrUtil.isNotBlank(query.getSortBy())) {
//            // 此时再去查 DB 就安全多了
//            Page<Item> dbPage = this.page(query.toMpPage("update_time", false));
//            return PageDTO.of(dbPage, ItemDTO.class);
//        }
//
//        // 1. 【第一次检查】尝试从 Redis ZSet 获取当前页的 ID 列表
//        PageDTO<ItemDTO> cacheResult = getPageFromRedis(start, size);
//        if (cacheResult != null) {
//            return cacheResult; // 缓存命中，直接返回
//        }
//
//        // 2. 缓存未命中（ZSet过期了），准备重建，获取互斥锁
//        String lockKey = "lock:rebuild:item:index";
//        RLock lock = redissonClient.getLock(lockKey);
//
//        try {
//            // 尝试获取锁，最多等3秒，拿到后10秒自动释放
//            if (lock.tryLock(3, 10, TimeUnit.SECONDS)) {
//
//                // 3. 🌟【第二次检查 (DCL)】🌟 拿到锁后，必须再查一次 Redis！
//                // 因为等待锁的过程中，前一个拿到锁的线程可能已经把缓存重建好了
//                cacheResult = getPageFromRedis(start, size);
//                if (cacheResult != null) {
//                    return cacheResult;
//                }
//
//                // 4. 确认缓存真的没了，安全地查询数据库，重建热点缓存（重建前500条）
//                rebuildHotItemCache();
//
//                // 5. 重建完成后，再次从 Redis 获取当页数据返回
//                return getPageFromRedis(start, size);
//            } else {
//                // 没有拿到锁，说明有别的线程正在疯狂重建缓存
//                // 休眠 50ms 后重试，避免给数据库带来压力
//                Thread.sleep(50);
//                return queryItemByPageWithCache(query);
//            }
//        } catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//            throw new RuntimeException("系统繁忙，请稍后再试");
//        } finally {
//            if (lock.isHeldByCurrentThread()) {
//                lock.unlock();
//            }
//        }
//    }
//
//    /**
//     * 从 Redis 中获取分页数据 (ZSet + MGET String)
//     */
//    private PageDTO<ItemDTO> getPageFromRedis(int start, int size) {
//        String indexKey = ItemCachePreloader.ITEM_INDEX_KEY;
//        int end = start + size - 1;
//
//        // 1. 查 ZSet 索引
//        Set<String> itemIds = stringRedisTemplate.opsForZSet().reverseRange(indexKey, start, end);
//        if (CollUtil.isEmpty(itemIds)) {
//            return null; // ZSet 没了，返回 null 触发 DCL 重建
//        }
//
//        // 2. 拿到 ID 后，批量 MGET 查详情
//        List<String> detailKeys = itemIds.stream()
//                .map(id -> ItemCachePreloader.ITEM_DETAIL_KEY_PREFIX + id)
//                .collect(Collectors.toList());
//        List<String> jsonItems = stringRedisTemplate.opsForValue().multiGet(detailKeys);
//
//        // 3. 🌟 【终极防御：缓存黑洞补偿机制】
//        List<ItemDTO> resultList = new ArrayList<>();
//        List<Long> missingIds = new ArrayList<>(); // 记录意外丢失详情的 ID
//
//        int index = 0;
//        for (String idStr : itemIds) {
//            String json = jsonItems.get(index++);
//            if (StrUtil.isNotBlank(json)) {
//                // 【新增】：如果是我们为了防穿透设置的空值标记，直接跳过，既不查DB也不返回给前端
//                if ("{}".equals(json)) {
//                    continue;
//                }
//                Item item = JSONUtil.toBean(json, Item.class);
//                resultList.add(BeanUtils.copyBean(item, ItemDTO.class));
//            } else {
//                missingIds.add(Long.valueOf(idStr)); // 发现黑洞！
//            }
//        }
//
////        // 如果发现部分商品详情被意外删除了（比如内存淘汰），去 DB 补齐并写回 Redis
////        if (CollUtil.isNotEmpty(missingIds)) {
////            List<Item> missingItems = this.listByIds(missingIds);
//////            for (Item item : missingItems) {
//////                resultList.add(BeanUtils.copyBean(item, ItemDTO.class));
//////                // 补齐详情，加上随机过期时间防雪崩
//////                stringRedisTemplate.opsForValue().set(
//////                        ItemCachePreloader.ITEM_DETAIL_KEY_PREFIX + item.getId(),
//////                        JSONUtil.toJsonStr(item),
//////                        60 + RandomUtil.randomInt(0, 10), TimeUnit.MINUTES
//////                );
//////            }
////            // 为了方便比对，将查到的真实数据转为 Map
////            Map<Long, Item> foundItemMap = missingItems.stream()
////                    .collect(Collectors.toMap(Item::getId, Function.identity()));
////            for (Long missingId : missingIds) {
////                Item item = foundItemMap.get(missingId);
////
////                if (item != null) {
////                    // 场景 A：DB 里有，属于正常的缓存过期，写回真实数据并加随机过期时间防雪崩
////                    resultList.add(BeanUtils.copyBean(item, ItemDTO.class));
////                    stringRedisTemplate.opsForValue().set(
////                            ItemCachePreloader.ITEM_DETAIL_KEY_PREFIX + item.getId(),
////                            JSONUtil.toJsonStr(item),
////                            60 + RandomUtil.randomInt(0, 10), TimeUnit.MINUTES
////                    );
////                } else {
////                    // 🌟 场景 B：DB 里也没有！触发缓存穿透防御机制
////                    // 缓存一个约定的空值（例如 "{}"），过期时间设置短一点（比如 3 分钟）
////                    // 这样接下来 3 分钟内，恶意请求再打过来，Redis 会直接挡掉，不会查 DB
////                    stringRedisTemplate.opsForValue().set(
////                            ItemCachePreloader.ITEM_DETAIL_KEY_PREFIX + missingId,
////                            "{}",
////                            3, TimeUnit.MINUTES
////                    );
////                }
////            }
////            // 注意：这里实际业务中可能需要对 resultList 重新按 missing 之前的顺序排个序
////        }
//        // 🌟 终极防御组合拳开始 🌟
//        if (CollUtil.isNotEmpty(missingIds)) {
//            // 1. 获取布隆过滤器
//            RBloomFilter<Long> bloomFilter = redissonClient.getBloomFilter(BloomFilterJobHandler.BLOOM_FILTER_KEY);
//            List<Long> passBloomFilterIds = new ArrayList<>();
//
//            for (Long missingId : missingIds) {
//                // 【第一道防线】：布隆过滤器
//                if (bloomFilter.isExists() && !bloomFilter.contains(missingId)) {
//                    // 布隆说绝对不存在！连DB都不用查，直接塞空值防穿透
//                    log.warn("布隆过滤器拦截了恶意或无效的请求ID: {}", missingId);
//                    stringRedisTemplate.opsForValue().set(
//                            ItemCachePreloader.ITEM_DETAIL_KEY_PREFIX + missingId,
//                            "{}",
//                            3, TimeUnit.MINUTES // 恶意攻击，短期封锁
//                    );
//                } else {
//                    // 布隆说可能存在（可能是真数据，也可能是误判，也可能是已下架的数据）
//                    passBloomFilterIds.add(missingId);
//                }
//            }
//
//            // 2. 只有通过了布隆过滤器的 ID，才有资格去查数据库
//            if (CollUtil.isNotEmpty(passBloomFilterIds)) {
//                List<Item> missingItems = this.listByIds(passBloomFilterIds);
//                Map<Long, Item> foundItemMap = missingItems.stream()
//                        .collect(Collectors.toMap(Item::getId, Function.identity()));
//
//                for (Long id : passBloomFilterIds) {
//                    Item item = foundItemMap.get(id);
//
//                    // 【业务补丁】：如果数据库里有，但业务状态是已下架（逻辑删除），也应该视为无效！
//                    boolean isValidItem = item != null && item.getStatus() == 1;
//
//                    if (isValidItem) {
//                        // 场景 A：数据真实且合法，写回 Redis 并加随机过期时间防雪崩
//                        resultList.add(BeanUtils.copyBean(item, ItemDTO.class));
//                        stringRedisTemplate.opsForValue().set(
//                                ItemCachePreloader.ITEM_DETAIL_KEY_PREFIX + item.getId(),
//                                JSONUtil.toJsonStr(item),
//                                60 + RandomUtil.randomInt(0, 10), TimeUnit.MINUTES
//                        );
//                    } else {
//                        // 【第二道防线】：兜底空值缓存
//                        // 场景 B1：DB里根本没有（属于布隆过滤器的 1% 误判率）
//                        // 场景 B2：DB里有，但是已经被商家下架了（逻辑删除了，布隆过滤器拦截不了）
//                        log.info("数据库兜底拦截，写入空值缓存。ID: {}", id);
//                        stringRedisTemplate.opsForValue().set(
//                                ItemCachePreloader.ITEM_DETAIL_KEY_PREFIX + id,
//                                "{}",
//                                3, TimeUnit.MINUTES // 种下短期空值补丁
//                        );
//                    }
//                }
//            }
//        }
//
//        // 4. 获取 ZSet 的总长度，用于组装 PageDTO
//        Long total = stringRedisTemplate.opsForZSet().zCard(indexKey);
//
//        // 组装返回给前端的分页对象
//        PageDTO<ItemDTO> pageDTO = new PageDTO<>();
//        pageDTO.setTotal(total != null ? total : 0L);
//        // 为了方便，这里假定热点缓存总共500条计算pages
//        pageDTO.setPages((total + size - 1) / size);
//        pageDTO.setList(resultList);
//
//        return pageDTO;
//    }

    /**
     * 重建前500条热点数据的缓存（逻辑与 Preloader 类似）
     */
    private void rebuildHotItemCache() {
        QueryWrapper<Item> queryWrapper = new QueryWrapper<>();
        queryWrapper.orderByDesc("update_time").last("LIMIT 500");
        List<Item> hotItems = this.list(queryWrapper);

        if (hotItems.isEmpty()) return;

        for (Item item : hotItems) {
            String detailKey = ItemCachePreloader.ITEM_DETAIL_KEY_PREFIX + item.getId();
            // 重建 Detail
            stringRedisTemplate.opsForValue().set(
                    detailKey, JSONUtil.toJsonStr(item),
                    60 + RandomUtil.randomInt(0, 10), TimeUnit.MINUTES);

            // 重建 ZSet
            long score = item.getUpdateTime() != null ?
                    item.getUpdateTime().toEpochSecond(java.time.ZoneOffset.of("+8")) : 0;
            stringRedisTemplate.opsForZSet().add(ItemCachePreloader.ITEM_INDEX_KEY, item.getId().toString(), score);
        }
        // ZSet TTL 设置为 30分钟，严格短于 Detail
        stringRedisTemplate.expire(ItemCachePreloader.ITEM_INDEX_KEY, 30, TimeUnit.MINUTES);
    }

    @Override
    public List<ItemDTO> queryItemByIds(Collection<Long> ids) {
        return BeanUtils.copyList(listByIds(ids), ItemDTO.class);
    }

    @Override
    @Transactional(rollbackFor = Exception.class) // 保证流水更新和库存恢复在同一个事务中
    public void increaseStock(Long orderId, List<OrderDetailDTO> orderDetailDTOS) {

        // 1. 第一道防线：利用库存流水的“状态机”做严密的幂等性校验
        // 只有状态为 1 (已扣减) 的流水，才能被修改为 2 (已回滚)
        int affectedRows = stockDeductLogMapper.update(null,
                new LambdaUpdateWrapper<StockDeductLog>()
                        .set(StockDeductLog::getStatus, 2) // 2: 已回滚/已恢复
                        .eq(StockDeductLog::getOrderId, orderId)
                        .eq(StockDeductLog::getStatus, 1)  // 【核心条件】必须是 1 才能改
        );

        // 2. 幂等拦截处理
        if (affectedRows == 0) {
            // affectedRows 为 0 说明什么？
            // 情况 A：流水不存在（压根没扣过库存，凭什么给你加？）
            // 情况 B：流水状态已经是 2 了（MQ 重复发来的回滚消息，之前已经加过了）
            // 这两种情况我们都直接放行（return），绝对不去改真实库存！这就叫完美幂等！
            log.warn("恢复库存触发幂等拦截：该订单 {} 无有效扣减记录或已完成恢复，直接放行", orderId);
            return;
        }

        // 3. 安全恢复真实库存
//        for (OrderDetailDTO orderDetailDTO : orderDetailDTOS) {
//            // 调用底层的原子累加 SQL，不再使用先查后改的危险操作
//            baseMapper.increaseStockSafe(orderDetailDTO.getItemId(), orderDetailDTO.getNum());
//        }
        // 批量加库存没有条件限制，但也要校验行数防止出现脏数据
        int affectedIncreaseRows = baseMapper.batchIncreaseStockSafe(orderDetailDTOS);
        if (affectedIncreaseRows != orderDetailDTOS.size()) {
            log.error("订单 {} 批量恢复库存异常，请检查商品库是否完整！", orderId);
            throw new RuntimeException("批量恢复数据库库存异常");
        }
        for (OrderDetailDTO item : orderDetailDTOS) {
            itemStockVersionService.recordStockChange(item.getItemId(), orderId, "RESTORE");
        }
        // 3. 【新增逻辑】安全恢复 Redis 缓存库存
        // 为什么用 afterCommit？
        // 如果我们直接在这里写 redis.increment，万一稍后 MySQL 事务因为某种原因回滚了，
        // Redis 的库存就多加了，造成超卖！所以必须等 DB 彻底提交成功再去操作 Redis。
//        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
//            @Override
//            public void afterCommit() {
////                try {
////                    for (OrderDetailDTO detail : orderDetailDTOS) {
////                        String redisKey = "item:stock:" + detail.getItemId();
////                        // 在 Redis 中原子增加对应的库存数量
////                        stringRedisTemplate.opsForValue().increment(redisKey, detail.getNum());
////                    }
////                    log.info("订单 {} 关单退库，Redis 缓存库存恢复成功", orderId);
////                } catch (Exception e) {
////                    // 如果 Redis 挂了导致加库存失败怎么办？
////                    // 大厂兜底：记录错误日志/告警，通过定时任务(Canal或定时查流水)进行异步对账修复
////                    log.error("订单 {} MySQL退库成功，但 Redis 缓存库存恢复失败，需要人工/脚本介入比对", orderId, e);
////                }
//                try {
//                    // 组装 KEYS 和 ARGS
//                    List<String> keys = new ArrayList<>(orderDetailDTOS.size());
//                    Object[] args = new Object[orderDetailDTOS.size()];
//                    for (int i = 0; i < orderDetailDTOS.size(); i++) {
//                        keys.add("item:stock:" + orderDetailDTOS.get(i).getItemId());
//                        args[i] = String.valueOf(orderDetailDTOS.get(i).getNum());
//                    }
//
//                    // 【一次网络 I/O，原子批量执行】
//                    stringRedisTemplate.execute(BATCH_INCR_SCRIPT, keys, args);
//
//                    log.info("订单 {} 关单退库，Redis 缓存库存批量恢复成功", orderId);
//                } catch (Exception e) {//如果实在要保证redis库存不丢失，应该引入canal去维护数据库与redis库存一致性
//                    log.error("订单 {} Redis 批量恢复失败", orderId, e);
//                }
//            }
//        });
        log.info("订单 {} 超时/取消，恢复底层真实库存成功并更新流水状态", orderId);
    }
}
