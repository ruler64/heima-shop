package com.hmall.item.config;

import cn.hutool.core.util.RandomUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmall.item.domain.po.Item;
import com.hmall.item.service.IItemService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class ItemCachePreloader implements ApplicationRunner {

    private final IItemService itemService;
    private final StringRedisTemplate stringRedisTemplate;

    // 定义缓存 Key
    public static final String ITEM_INDEX_KEY = "item:index:default";
    public static final String ITEM_DETAIL_KEY_PREFIX = "item:detail:";

    @Override
    public void run(ApplicationArguments args) {
        log.info("============== 开始预热商品缓存 ==============");
        // 1. 查询默认排序（update_time降序）的前 500 条热点数据
        QueryWrapper<Item> queryWrapper = new QueryWrapper<>();
        queryWrapper.orderByDesc("update_time").last("LIMIT 500");
        List<Item> hotItems = itemService.list(queryWrapper);

        if (hotItems.isEmpty()) return;

        // 2. 遍历写入 ZSet 索引和 String 详情
        for (Item item : hotItems) {
            String detailKey = ITEM_DETAIL_KEY_PREFIX + item.getId();

            // 写入详情 (TTL: 60分钟)
            stringRedisTemplate.opsForValue().set(
                    detailKey,
                    JSONUtil.toJsonStr(item),
                    60+ RandomUtil.randomInt(0, 10), TimeUnit.MINUTES
            );

            // 写入 ZSet 索引 (Score用更新时间戳，TTL: 30分钟)
            long score = item.getUpdateTime() != null ?
                    item.getUpdateTime().toEpochSecond(java.time.ZoneOffset.of("+8")) : 0;
            stringRedisTemplate.opsForZSet().add(ITEM_INDEX_KEY, item.getId().toString(), score);
        }

        // 设置 ZSet 的整体过期时间 (30分钟)
        stringRedisTemplate.expire(ITEM_INDEX_KEY, 30, TimeUnit.MINUTES);
        log.info("============== 商品缓存预热完成，共加载 {} 条 ==============", hotItems.size());
    }
}