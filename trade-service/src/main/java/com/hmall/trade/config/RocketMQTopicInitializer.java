package com.hmall.trade.config;

import com.hmall.trade.constants.MQConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.Set;

/**
 * RocketMQ Topic 自动初始化。
 * 由于要用到网络IO
 * 在服务启动时检查并创建所需 Topic，保证无论 Broker 是否开启
 * autoCreateTopicEnable，业务 Topic 都存在且队列数符合预期。
 */
@Slf4j
@Component
public class RocketMQTopicInitializer implements ApplicationRunner {

    @Value("${rocketmq.name-server}")
    private String nameServer;

    @Value("${rocketmq.cluster-name:DefaultCluster}")
    private String clusterName;

    // 与消费者线程数对齐，保证每个线程至少有一个队列可消费
    private static final int QUEUE_NUM = 8;
    // 死信队列数固定为 1（死信量极少，不需要多队列并发）
    private static final int DEAD_QUEUE_NUM = 1;

    @Override
    public void run(ApplicationArguments args) {
        DefaultMQAdminExt admin = new DefaultMQAdminExt();
        admin.setNamesrvAddr(nameServer);
        admin.setAdminExtGroup("trade-topic-initializer");

        try {
            admin.start();

            // 需要创建的所有 Topic
            createTopicIfAbsent(admin, MQConstants.ROCKETMQ_ORDER_TOPIC, QUEUE_NUM);

            // 死信 Topic：固定命名规则 %DLQ%{consumerGroup}
            // 队列数固定为 1（死信量极少，不需要多队列并发）
            createTopicIfAbsent(admin,
                    "%DLQ%" + MQConstants.ROCKETMQ_ORDER_CONSUMER_GROUP, DEAD_QUEUE_NUM);

            createTopicIfAbsent(admin, MQConstants.ROCKETMQ_CANCEL_TOPIC, 8);
            createTopicIfAbsent(admin, "%DLQ%" + MQConstants.ROCKETMQ_CANCEL_CONSUMER_GROUP, 1);

        } catch (Exception e) {
            // Topic 初始化失败不应阻断服务启动（Broker 可能未就绪），打印告警即可
            // 可根据实际情况改为抛出异常阻断启动
            log.error("[RocketMQ] Topic 初始化失败，请检查 Broker 连接。nameServer={}", nameServer, e);
        } finally {
            admin.shutdown();
        }
    }

    /**
     * 检查 Topic 是否存在，不存在则创建。
     * 已存在时不做任何操作，保证幂等。
     */
    private void createTopicIfAbsent(DefaultMQAdminExt admin, String topic, int queueNum) {
        try {
            // 能查到路由信息说明 Topic 已存在
            admin.examineTopicRouteInfo(topic);
            log.info("[RocketMQ] Topic 已存在，跳过创建：{}", topic);

        } catch (Exception e) {
            // 查不到路由 = Topic 不存在，执行创建
            try {
                // 获取集群中所有 Broker 地址
                Set<String> brokerAddresses = CommandUtil.fetchMasterAddrByClusterName(
                        admin, clusterName);

                for (String brokerAddr : brokerAddresses) {
                    admin.createAndUpdateTopicConfig(brokerAddr,
                            buildTopicConfig(topic, queueNum));
                }
                log.info("[RocketMQ] Topic 创建成功：{}，队列数：{}", topic, queueNum);

            } catch (Exception ex) {
                log.error("[RocketMQ] Topic 创建失败：{}", topic, ex);
            }
        }
    }

    private TopicConfig buildTopicConfig(String topic, int queueNum) {
        TopicConfig config = new TopicConfig();
        config.setTopicName(topic);
        config.setReadQueueNums(queueNum);
        config.setWriteQueueNums(queueNum);
        // 普通权限：可读可写
        config.setPerm(6);
        return config;
    }
}