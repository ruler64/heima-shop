package com.hmall.cart.config;

import com.hmall.cart.constants.MQConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.Set;

@Slf4j
@Component
public class CartRocketMQTopicInitializer implements ApplicationRunner {

    @Value("${rocketmq.name-server}")
    private String nameServer;

    @Value("${rocketmq.cluster-name:DefaultCluster}")
    private String clusterName;

    @Override
    public void run(ApplicationArguments args) {
        DefaultMQAdminExt admin = new DefaultMQAdminExt();
        admin.setNamesrvAddr(nameServer);
        admin.setAdminExtGroup("cart-topic-initializer");

        try {
            admin.start();

            // cart 订阅 TRADE_DB_ORDER_TOPIC 清空购物车
            createTopicIfAbsent(admin, MQConstants.ROCKETMQ_DB_ORDER_TOPIC, 8);
            createTopicIfAbsent(admin, "%DLQ%"+MQConstants.ROCKETMQ_CART_CLEAR_GROUP, 1);

        } catch (Exception e) {
            log.error("[RocketMQ] cart-service Topic 初始化失败", e);
        } finally {
            admin.shutdown();
        }
    }

    private void createTopicIfAbsent(DefaultMQAdminExt admin, String topic, int queueNum) {
        try {
            admin.examineTopicRouteInfo(topic);
            log.info("[RocketMQ] Topic 已存在：{}", topic);
        } catch (Exception e) {
            try {
                Set<String> brokerAddresses = CommandUtil
                        .fetchMasterAddrByClusterName(admin, clusterName);
                for (String addr : brokerAddresses) {
                    TopicConfig config = new TopicConfig();
                    config.setTopicName(topic);
                    config.setReadQueueNums(queueNum);
                    config.setWriteQueueNums(queueNum);
                    config.setPerm(6);
                    admin.createAndUpdateTopicConfig(addr, config);
                }
                log.info("[RocketMQ] Topic 创建成功：{}", topic);
            } catch (Exception ex) {
                log.error("[RocketMQ] Topic 创建失败：{}", topic, ex);
            }
        }
    }
}