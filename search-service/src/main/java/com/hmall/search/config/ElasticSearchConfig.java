package com.hmall.search.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RestHighLevelClient 选型理由（vs Spring Data ES）：
 *  1. routing 精确控制：IndexRequest/UpdateRequest.routing(userId) 无歧义
 *  2. 局部 patch：UpdateRequest.doc(map) 天然 partial update，不会全量覆盖
 *  3. nested 查询：后续 NestedQueryBuilder + InnerHits 需手写 DSL，不受 Spring Data 抽象限制
 */
@Configuration
public class ElasticSearchConfig {

    @Value("${elasticsearch.host:192.168.31.128}")
    private String host;

    @Value("${elasticsearch.port:9200}")
    private int port;

    @Bean
    public RestHighLevelClient restHighLevelClient() {
        return new RestHighLevelClient(
                RestClient.builder(new HttpHost(host, port, "http"))
                        .setRequestConfigCallback(cfg -> cfg
                                .setConnectTimeout(5_000)
                                .setSocketTimeout(30_000))
                        .setFailureListener(new RestClient.FailureListener() {

                            @Override
                            // 【修复处】：ES 7.x 之后，这里的参数从 HttpHost 变成了 Node
                            public void onFailure(Node node) {
                                // 可接 Micrometer Counter 上报 es_node_failure_total
                                // 例如: log.error("ES Node failed: {}", node.getHost());
                            }
                        })
        );
    }
}