package com.hmall.item.task;

import com.hmall.item.domain.po.Item;
import com.hmall.item.service.IItemService;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class BloomFilterJobHandler {

    private final IItemService itemService;
    private final RedissonClient redissonClient;

    public static final String BLOOM_FILTER_KEY = "bloom:item:id";
    public static final String BLOOM_FILTER_TEMP_KEY = "bloom:item:id:temp";

    @XxlJob("rebuildItemBloomFilter")
    public void rebuildBloomFilter() {
        XxlJobHelper.log("开始重建商品布隆过滤器...");
        long start = System.currentTimeMillis();

        // 1. 初始化一个临时的布隆过滤器 (预计容量100万，误差率1%)
        RBloomFilter<Long> tempBloomFilter = redissonClient.getBloomFilter(BLOOM_FILTER_TEMP_KEY);
        tempBloomFilter.tryInit(1000000L, 0.01);

        // 2. 分批查询数据库中【未被逻辑删除且已上架】的真实商品 ID
        // 注意：千万不要一次性查出上百万条数据，要分批查！这里为了简化演示逻辑
        List<Item> validItems = itemService.lambdaQuery()
                .eq(Item::getStatus, 1) // 1表示正常上架，2表示下架,3逻辑删除
                .select(Item::getId) // 只需要查ID，极大地节省内存
                .list();

        for (Item item : validItems) {
            tempBloomFilter.add(item.getId());
        }

        // 3. 🌟 【无缝切换核心】利用 Redis 的 RENAME 指令，将临时过滤器重命名为正式过滤器
        // rename 是原子操作，这样线上请求不会因为布隆过滤器正在重建而报错
        tempBloomFilter.rename(BLOOM_FILTER_KEY);

        XxlJobHelper.log("布隆过滤器重建完成，耗时: {}ms，载入合法ID数: {}", (System.currentTimeMillis() - start), validItems.size());
    }
}