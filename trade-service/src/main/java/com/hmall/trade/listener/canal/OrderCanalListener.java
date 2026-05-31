/*
package com.hmall.trade.listener.canal;

import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import com.hmall.trade.constants.MQConstants;
import com.hmall.trade.domain.document.EsOrderDoc;
import com.hmall.trade.domain.po.Order;
import com.hmall.trade.domain.po.OrderDetail;
import com.hmall.trade.service.IOrderDetailService;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;

*/
/**
 * Canal Binlog 监听器：负责实现 Order 数据库到 Elasticsearch 的实时同步
 *//*

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderCanalListener {

    private final RestHighLevelClient restHighLevelClient;
    private final IOrderDetailService orderDetailService; // 注入订单详情服务

    private static final String ORDER_INDEX = "order_index";

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = MQConstants.ORDER_CANAL_SYNC_ES_QUEUE, durable = "true"),
            exchange = @Exchange(name = MQConstants.ORDER_CANAL_RABBITMQ_EXCHANGE, type = ExchangeTypes.TOPIC),
            key = MQConstants.ORDER_CANAL_CHANGE_KEY
    ))
    public void onOrderInsert(Message message, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        try {
            String payload = new String(message.getBody());
            JSONObject jsonObject = JSON.parseObject(payload);

            String table = jsonObject.getString("table");
            String type = jsonObject.getString("type");
            log.info("收到订单 Canal 消息，表名：{}, 类型：{}", table, type);

            // 只处理 order 表的数据
            if ("order".equals(table)) {
                JSONArray dataArray = jsonObject.getJSONArray("data");
                JSONArray oldArray = jsonObject.getJSONArray("old");

                for (int i = 0; i < dataArray.size(); i++) {
                    JSONObject rowData = dataArray.getJSONObject(i);
                    JSONObject oldData = oldArray != null ? oldArray.getJSONObject(i) : new JSONObject();
                    
                    // 将 JSON 转换为实体类
                    Order order = rowData.toJavaObject(Order.class);
                    String orderIdStr = order.getId().toString();
                    String routingKey = order.getUserId().toString();

                    if ("INSERT".equals(type)) {
                        // 1. 处理新增订单 (INSERT)
                        // 查询订单关联的商品详情
                        List<OrderDetail> details = orderDetailService.lambdaQuery()
                                .eq(OrderDetail::getOrderId, order.getId())
                                .list();

                        // 组装宽表文档
                        EsOrderDoc esDoc = new EsOrderDoc(order, details);

                        // 写入 ES (带 Routing)
                        IndexRequest indexRequest = new IndexRequest(ORDER_INDEX)
                                .id(orderIdStr)
                                .routing(routingKey) // 🌟 核心：按照 userId 路由
                                .source(JSONUtil.toJsonStr(esDoc), XContentType.JSON);

                        restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
                        log.info("成功同步新订单到 ES，订单号: {}, Routing User: {}", orderIdStr, routingKey);

                    } else if ("UPDATE".equals(type)) {
                        // 2. 处理订单更新 (UPDATE)
                        // 依据你的业务规则：如果是 update 并且 oldData 中包含 status 字段，说明状态发生变更
                        if (oldData.containsKey("status")) {
                            
                            // 局部更新 ES 中的文档状态 (带 Routing)
                            UpdateRequest updateRequest = new UpdateRequest(ORDER_INDEX, orderIdStr)
                                    .routing(routingKey) // 🌟 核心：更新时也必须带上同样的路由键，否则找不到数据
                                    .doc("status", order.getStatus());
                                    // 如果有其他时间如 payTime、updateTime 也需要更新，可以在此一并追加
                                    // .doc("status", order.getStatus(), "payTime", order.getPayTime());

                            restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
                            log.info("成功同步订单状态变更到 ES，订单号: {}, 状态更新为: {}", orderIdStr, order.getStatus());
                        }
                    }
                }
            }

            // 处理成功，手动 ACK
            channel.basicAck(deliveryTag, false);

        } catch (Exception e) {
            log.error("处理 Order Canal 消息同步 ES 失败，准备重试", e);
            // 发生异常，NACK 让消息重回队列，无限重试直到同步成功
            channel.basicNack(deliveryTag, false, true);
        }
    }
}*/
