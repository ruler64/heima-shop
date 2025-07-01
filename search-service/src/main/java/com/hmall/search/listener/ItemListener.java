package com.hmall.search.listener;

import cn.hutool.json.JSONUtil;
import com.hmall.api.client.ItemClient;
import com.hmall.search.constants.MQConstants;
import com.hmall.search.domain.po.ItemDoc;

import com.hmall.api.dto.ItemDTO;
import com.hmall.common.utils.BeanUtils;
import lombok.RequiredArgsConstructor;
import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.*;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
public class ItemListener {

//    private final ISearchService searchService;
    private final ItemClient itemClient;
    private RestHighLevelClient client;

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = MQConstants.LAZY_ITEM_ADD_QUEUE_NAME,durable = "true"),
            exchange = @Exchange(name = MQConstants.LAZY_EXCHANGE_NAME,type = ExchangeTypes.DIRECT),
            arguments = @Argument(name = "x-queue-mode", value = "lazy"), //声明惰性队列，持久化存储，更好的性能
            key = {MQConstants.LAZY_ITEM_ADD_KEY}
    ))
    public void listenAddItem(Long itemId) throws IOException {//若id不存在则新增文档，若id存在则全量修改文档
        ItemDTO itemDTO = itemClient.queryItemById(itemId);
        // 0.把监听到的更新数据转为文档数据
        ItemDoc itemDoc = BeanUtils.copyProperties(itemDTO, ItemDoc.class);
        // 1.准备Request
        IndexRequest request = new IndexRequest("items").id(itemDoc.getId());
        // 2.准备JSON文档
        request.source(JSONUtil.toJsonStr(itemDoc), XContentType.JSON);
        // 3.发送请求
        client.index(request, RequestOptions.DEFAULT);
    }

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = MQConstants.LAZY_ITEM_UPDATE_QUEUE_NAME,durable = "true"),
            exchange = @Exchange(name = MQConstants.LAZY_EXCHANGE_NAME,type = ExchangeTypes.DIRECT),
            arguments = @Argument(name = "x-queue-mode", value = "lazy"), //声明惰性队列，持久化存储，更好的性能
            key = {MQConstants.LAZY_ITEM_UPDATE_KEY}
    ))
    public void listenUpdateItem(Long itemId) throws IOException {//若id不存在则新增文档，若id存在则全量修改文档
        ItemDTO itemDTO = itemClient.queryItemById(itemId);
        // 0.把监听到的更新数据转为文档数据
        ItemDoc itemDoc = BeanUtils.copyProperties(itemDTO, ItemDoc.class);
        itemDoc.setUpdateTime(LocalDateTime.now()); //设置更新时间
        // 1.准备Request
        IndexRequest request = new IndexRequest("items").id(itemDoc.getId());
        // 2.准备JSON文档
        request.source(JSONUtil.toJsonStr(itemDoc), XContentType.JSON);
        // 3.发送请求
        client.index(request, RequestOptions.DEFAULT);
    }

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = MQConstants.LAZY_ITEM_DELETE_QUEUE_NAME,durable = "true"),
            exchange = @Exchange(name = MQConstants.LAZY_EXCHANGE_NAME,type = ExchangeTypes.DIRECT),
            arguments = @Argument(name = "x-queue-mode", value = "lazy"), //声明惰性队列，持久化存储，更好的性能
            key = {MQConstants.LAZY_ITEM_DELETE_KEY}
    ))
    public void listenDeleteItem(Long itemId) throws IOException {//删除文档
        // 1.准备Request
        DeleteRequest request = new DeleteRequest("items", itemId.toString());
        // 2.发送请求
        client.delete(request, RequestOptions.DEFAULT);
    }

    @PostConstruct
    void setUp() {//创建
        client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.31.128:9200")
        ));
    }

    @PreDestroy
    void tearDown() throws IOException {//销毁
        if (client!=null){
            client.close();
        }
    }
}
