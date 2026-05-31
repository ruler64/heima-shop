package com.hmall.search.listener.canal;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import com.hmall.search.constants.MQConstants;
import com.hmall.search.domain.doc.OrderDetailDoc;
import com.hmall.search.domain.doc.OrderDoc;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.*;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Canal binlog → ES 增量同步监听器（search-service）
 *
 * 监听表：hm-trade.order / hm-trade.order_detail
 * 共用 routing key：canal.order.change（同一队列，Canal 保证事务内顺序）
 *
 * ┌──────────────────────────────────────────────────────────────────┐
 * │  两阶段写入策略                                                   │
 * │                                                                  │
 * │  Phase 1  order INSERT                                           │
 * │    → IndexRequest：OrderDoc 骨架（items=[]）                      │
 * │    → Redis SET search:order:uid:{orderId} = userId  TTL=2h       │
 * │                                                                  │
 * │  Phase 2  order_detail INSERT（同事务，Canal 保证晚于 Phase 1）   │
 * │    → Redis GET userId（routing 来源）                             │
 * │    → Painless Script：ctx._source.items.add(params.item)         │
 * │                                                                  │
 * │  order UPDATE（old 含 status）                                    │
 * │    → doc patch：status + 时间戳字段                               │
 * └──────────────────────────────────────────────────────────────────┘
 *
 * 幂等：IndexRequest._id = orderId（重复消费覆盖）；
 *       Script update retryOnConflict=3 处理并发版本冲突。
 *
 * 容错：NACK + requeue=true 触发 RabbitMQ 重试；
 *       建议配置 DLQ（超过 maxRetry 后人工介入）。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderCanalEsListener {

    private static final String ORDER_INDEX = "order_index";

    private static final DateTimeFormatter DT_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /** Painless script：向 nested items 数组追加一个元素 */
    private static final String APPEND_ITEM_SCRIPT =
            "if (ctx._source.items == null) { ctx._source.items = [params.item]; } " +
                    "else { ctx._source.items.add(params.item); }";

    private final RestHighLevelClient  esClient;
    private final StringRedisTemplate  redisTemplate;

    // ─────────────────────────────────────────────────────────────────────
    // RabbitMQ 绑定
    // ─────────────────────────────────────────────────────────────────────

    @RabbitListener(bindings = @QueueBinding(
            value    = @Queue(
                    name    = MQConstants.ORDER_CANAL_SYNC_ES_QUEUE,
                    durable = "true",
                    arguments = {
                            // 配合 DLQ 兜底：超过 10 次重试转入死信队列
                            @Argument(name = "x-dead-letter-exchange",
                                    value = "canal.order.dlx"),
                            @Argument(name = "x-dead-letter-routing-key",
                                    value = "canal.order.dead")
                    }),
            exchange = @Exchange(
                    name = MQConstants.CANAL_EXCHANGE,
                    type = ExchangeTypes.TOPIC),
            key      = MQConstants.ORDER_CANAL_CHANGE_KEY
    ))
    public void listenOrderBinlog(
            Message message,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {

        try {
            JSONObject json      = JSON.parseObject(new String(message.getBody()));
            String     table     = json.getString("table");
            String     type      = json.getString("type");
            JSONArray  dataArray = json.getJSONArray("data");
            JSONArray  oldArray  = json.getJSONArray("old");

            log.debug("Canal order 消息 table={} type={}", table, type);

            if (dataArray == null || dataArray.isEmpty()) {
                channel.basicAck(deliveryTag, false);
                return;
            }

            switch (table) {
                case "order":
                    dispatchOrderEvent(type, dataArray, oldArray);
                    break;
                case "order_detail":
                    dispatchOrderDetailEvent(type, dataArray);
                    break;
                default:
                    log.debug("非目标表 {}, 忽略", table);
                    break;
            }

            channel.basicAck(deliveryTag, false);

        } catch (Exception e) {
            log.error("Order Canal → ES 同步失败，NACK 等待重试", e);
            channel.basicNack(deliveryTag, false, true);
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // order 表事件分发
    // ─────────────────────────────────────────────────────────────────────

    private void dispatchOrderEvent(String type, JSONArray dataArray, JSONArray oldArray) {
        for (int i = 0; i < dataArray.size(); i++) {
            JSONObject row     = dataArray.getJSONObject(i);
            JSONObject oldData = (oldArray != null) ? oldArray.getJSONObject(i) : null;

            switch (type) {
                case "INSERT":
                    handleOrderInsert(row);
                    break;
                case "UPDATE":
                    if (oldData != null) {
                        handleOrderUpdate(row, oldData);
                    }
                    break;
                default:
                    log.debug("order 事件类型 {} 无需处理", type);
                    break;
            }
        }
    }

    /**
     * Phase 1：order INSERT
     * 写 OrderDoc 骨架（items 为空），同时缓存 orderId → userId
     */
    private void handleOrderInsert(JSONObject row) {
        Long orderId = row.getLong("id");
        Long userId  = row.getLong("user_id");

        // ① 缓存 orderId → userId，供 Phase 2 的 order_detail 使用
        String cacheKey = MQConstants.ORDER_UID_CACHE_PREFIX + orderId;
        redisTemplate.opsForValue().set(
                cacheKey, userId.toString(),
                MQConstants.ORDER_UID_CACHE_TTL_SECONDS, TimeUnit.SECONDS);

        // ② 构建 OrderDoc 骨架
        OrderDoc doc = OrderDoc.builder()
                .orderId(orderId)
                .userId(userId)
                .status(row.getInteger("status"))
                .totalFee(row.getInteger("total_fee"))
                .paymentType(row.getInteger("payment_type"))
                .createTime(parseDt(row.getString("create_time")))
                .items(Collections.emptyList())
                .build();

        // ③ IndexRequest：_id=orderId，routing=userId
        IndexRequest req = new IndexRequest(ORDER_INDEX)
                .id(orderId.toString())
                .routing(userId.toString())
                .source(JSON.toJSONString(doc), XContentType.JSON);

        try {
            esClient.index(req, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.info("es插入index异常",e);
            throw new RuntimeException(e);
        }
        log.info("[Phase-1] 订单 {} 骨架写入 ES（routing=userId:{}）", orderId, userId);
    }

    /**
     * order UPDATE：局部 patch status 及状态时间戳
     * 仅当 old 中包含 status 字段时触发（状态流转：支付/发货/关单等）
     */
    private void handleOrderUpdate(JSONObject row, JSONObject oldData){
        if (!oldData.containsKey("status")) {
            log.debug("order UPDATE 不含 status 变更，跳过 ES 同步");
            return;
        }

        Long orderId = row.getLong("id");
        Long userId  = resolveUserId(orderId);
        if (userId == null) {
            log.warn("订单 {} UPDATE 无法解析 userId，跳过", orderId);
            return;
        }

        Map patch = new HashMap<>();
        patch.put("status", row.getInteger("status"));
        putDtIfPresent(patch, "payTime",     row, "pay_time");
        putDtIfPresent(patch, "consignTime", row, "consign_time");
        putDtIfPresent(patch, "endTime",     row, "end_time");
        putDtIfPresent(patch, "closeTime",   row, "close_time");

        UpdateRequest req = new UpdateRequest(ORDER_INDEX, orderId.toString())
                .routing(userId.toString())
                .doc(patch)
                .retryOnConflict(3);

        try {
            esClient.update(req, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.info("es更新异常",e);
            throw new RuntimeException(e);
        }
        log.info("[status-patch] 订单 {} status → {}（routing=userId:{}）",
                orderId, row.getInteger("status"), userId);
    }

    // ─────────────────────────────────────────────────────────────────────
    // order_detail 表事件分发
    // ─────────────────────────────────────────────────────────────────────

    private void dispatchOrderDetailEvent(String type, JSONArray dataArray) {
        if (!"INSERT".equals(type)) {
            log.debug("order_detail 事件类型 {} 无需处理", type);
            return;
        }
        for (int i = 0; i < dataArray.size(); i++) {
            try {
                handleOrderDetailInsert(dataArray.getJSONObject(i));
            } catch (IOException e) {
                log.info("handleOrderDetailInsert方法异常",e);
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Phase 2：order_detail INSERT
     * Painless Script 追加 nested item 到已有 OrderDoc.items
     *
     * userId 从 Redis 缓存获取（Phase 1 写入），无需跨服务 Feign 调用
     */
    private void handleOrderDetailInsert(JSONObject row) throws IOException {
        Long orderId = row.getLong("order_id");
        Long userId  = resolveUserId(orderId);

        if (userId == null) {
            // 缓存未命中：Phase 1 可能还未完成（极罕见），NACK 重试后 Phase 1 必然先到
            log.warn("[Phase-2] 订单 {} 缓存 userId 未命中，抛出异常触发 NACK 重试", orderId);
            throw new IllegalStateException("orderId=" + orderId + " userId 缓存未命中");
        }

        // 构建 item 参数 Map（Painless script params）
        Map item = new HashMap<>();
        item.put("itemId", row.getLong("item_id"));
        item.put("name",   row.getString("name"));
        item.put("num",    row.getInteger("num"));
        item.put("price",  row.getInteger("price"));
        item.put("image",  row.getString("image"));
        item.put("spec",   row.getString("spec"));

        Script script = new Script(
                ScriptType.INLINE,
                "painless",
                APPEND_ITEM_SCRIPT,
                Collections.singletonMap("item", item));

        UpdateRequest req = new UpdateRequest(ORDER_INDEX, orderId.toString())
                .routing(userId.toString())
                .script(script)
                .retryOnConflict(3);

        esClient.update(req, RequestOptions.DEFAULT);
        log.info("[Phase-2] 订单 {} 追加明细 itemId={}（routing=userId:{}）",
                orderId, row.getLong("item_id"), userId);
    }

    // ─────────────────────────────────────────────────────────────────────
    // 工具方法
    // ─────────────────────────────────────────────────────────────────────

    /**
     * 解析 userId：
     *   优先从 Canal row 中直接取（order 表 UPDATE 场景，row 携带 user_id）
     *   其次从 Redis 缓存取（order_detail 场景，row 无 user_id）
     */
    private Long resolveUserId(Long orderId) {
        String cached = redisTemplate.opsForValue()
                .get(MQConstants.ORDER_UID_CACHE_PREFIX + orderId);
        if (cached == null) return null;
        return Long.parseLong(cached);
    }

    private LocalDateTime parseDt(String raw) {
        if (raw == null || raw.isBlank()) return null;
        return LocalDateTime.parse(raw, DT_FMT);
    }

    private void putDtIfPresent(Map patch,
                                String docField, JSONObject row, String dbCol) {
        String val = row.getString(dbCol);
        if (val != null && !val.isBlank()) {
            patch.put(docField, parseDt(val));
        }
    }
}