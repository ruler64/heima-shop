package com.hmall.item.config;

import com.hmall.item.constants.MQConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Set;

/**
 * item-service 启动时确保消费的 Topic 已存在。
 * 与 trade-service 的 Initializer 逻辑相同，只创建 item-service 关心的 Topic。
 */
@Slf4j
@Component
@Order(10) // 在 EpochInitializer(Order=2) 之后执行，不影响库存预热
public class ItemRocketMQTopicInitializer implements ApplicationRunner {

    @Value("${rocketmq.name-server}")
    private String nameServer;

    @Value("${rocketmq.cluster-name:DefaultCluster}")
    private String clusterName;

    @Override
    public void run(ApplicationArguments args) {
        DefaultMQAdminExt admin = new DefaultMQAdminExt();
        admin.setNamesrvAddr(nameServer);
        admin.setAdminExtGroup("item-topic-initializer");

        try {
            admin.start();
            // item-service 消费的 Topic
            createTopicIfAbsent(admin, MQConstants.ROCKETMQ_CANCEL_TOPIC, 8);
            createTopicIfAbsent(admin, "%DLQ%"+MQConstants.ROCKETMQ_CANCEL_CONSUMER_GROUP, 1);
        } catch (Exception e) {
            log.error("[RocketMQ] item-service Topic 初始化失败", e);
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
                log.info("[RocketMQ] Topic 创建成功：{}，队列数：{}", topic, queueNum);
            } catch (Exception ex) {
                log.error("[RocketMQ] Topic 创建失败：{}", topic, ex);
            }
        }
    }
}