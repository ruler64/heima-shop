package com.hmall.item.listener;

import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.hmall.item.config.ItemCachePreloader;
import com.hmall.item.domain.po.Item;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Canal Binlog 监听器：负责实现 DB 到 Redis 的最终一致性
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ItemCanalListener {

    private final StringRedisTemplate stringRedisTemplate;

    @RabbitListener(bindings = @QueueBinding(
            // 监听 Canal 推送 binlog 的专属队列
            value = @Queue(name = "canal.item.sync.queue", durable = "true"),
            exchange = @Exchange(name = "canal.exchange", type = ExchangeTypes.TOPIC),
            key = "canal.update.item"
    ))
    public void listenItemBinlog(String payload, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        try {
            JSONObject jsonObject = JSON.parseObject(payload);

            // 1. 获取变更表名和变更类型
            String table = jsonObject.getString("table");
            String type = jsonObject.getString("type");

            // 2. 只关心 item 表的 UPDATE 动作
            if ("item".equals(table) && "UPDATE".equals(type)) {

                // data 数组里包含了这行数据变更后的【最终全量字段】
                JSONArray dataArray = jsonObject.getJSONArray("data");
                // old 数组记录了本次 UPDATE 到底改了哪些字段 (增量变更)
                JSONArray oldArray = jsonObject.getJSONArray("old");

                for (int i = 0; i < dataArray.size(); i++) {
                    JSONObject rowData = dataArray.getJSONObject(i);
                    JSONObject oldData = oldArray != null ? oldArray.getJSONObject(i) : new JSONObject();
                    Long itemId = rowData.getLong("id");

                    // 🌟 核心防御：如果【仅仅】修改了库存，直接拦截忽略防超卖
                    if (oldData.containsKey("stock") && oldData.size() == 1) {
                        // 如果 old 对象里只有 stock 一个键，说明这是一次纯粹的扣减库存引发的 SQL
                        // 警告：由于 Redis 才是库存的主库，我们绝对不能用 DB 的库存去覆盖 Redis！
                        log.debug("Canal 仅监听到库存字段变更，忽略同步以防超卖！商品ID: {}", itemId);
                        continue;
                    }

                    // 🌟 静态属性覆盖
                    if (itemId != null) {
                        // 【修复 1】：强一致性！直接引用 ItemServiceImpl 也在使用的统一定义常量
                        String detailRedisKey = ItemCachePreloader.ITEM_DETAIL_KEY_PREFIX + itemId;

                        // 【修复 2】：格式对齐！将 Canal 的底层蛇形 JSON，反序列化为标准的 Item 实体类
                        // 这一步 Fastjson 会自动帮我们完成 create_time -> createTime 的映射
                        Item item = rowData.toJavaObject(Item.class);

                        // 【修复 3】：序列化对齐！使用与 ItemServiceImpl 完全相同的 Hutool JSONUtil 再次转为 JSON 字符串
                        // 保证 Redis 里的 JSON 结构 100% 相同，彻底解决反序列化报错问题
                        // rowData 里面有这行数据完整的、最新的所有字段
                        // 我们直接将这整行数据转成 JSON，覆盖掉 Redis 里的详情缓存！
                        // 这样天然具备幂等性，不怕消息重复消费。
                        stringRedisTemplate.opsForValue().set(detailRedisKey, JSONUtil.toJsonStr(item));

                        log.info("Canal 监听到商品 {} 的静态属性发生变更，已按照统一样式成功刷新 Redis 详情缓存", itemId);
                    }
                }
            }

            // 3. 处理成功，手动 ACK
            channel.basicAck(deliveryTag, false);

        } catch (Exception e) {
            log.error("处理 Canal Binlog 消息同步 Redis 失败，准备重试", e);
            // 发生异常，NACK 让消息重回队列，无限重试直到同步成功
            channel.basicNack(deliveryTag, false, true);
        }
    }
}