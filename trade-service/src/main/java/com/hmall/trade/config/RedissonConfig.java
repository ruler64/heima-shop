package com.hmall.trade.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedissonConfig {

    // 冒号后面加上默认的虚拟机 IP，防止 yaml 中未配置时报错
    @Value("${spring.redis.host:192.168.31.128}")
    private String host;

    // 冒号后面加上默认端口 6379（集群模式下这个变量其实没用到，但加上防报错）
    @Value("${spring.redis.port:6379}")
    private String port;

    // 冒号后面为空表示默认没有密码
    @Value("${spring.redis.password:}")
    private String password;

    @Bean
    public RedissonClient redissonClient() {
        Config config = new Config();

        // ========================== 之前的单节点配置 (已注释) ==========================
        /*
        // Redis url 必须以 redis:// 或 rediss:// (SSL) 开头
        String redisAddress = String.format("redis://%s:%s", host, port);

        // 配置单节点模式
        config.useSingleServer()
                .setAddress(redisAddress)
                .setDatabase(0);

        // 如果有密码则设置密码
        if (password != null && !password.isEmpty()) {
            config.useSingleServer().setPassword(password);
        }
        */
        // ============================================================================

        // ========================== 集群模式配置 ==========================
        // 集群模式下，addNodeAddress 可以添加多个节点地址
        // Redisson 会自动发现集群中的其他节点，但建议至少把所有 Master 节点都写上
        config.useClusterServers()
                .setScanInterval(2000) // 集群状态扫描间隔，单位是毫秒
                .addNodeAddress(
                        "redis://" + host + ":7001",
                        "redis://" + host + ":7002",
                        "redis://" + host + ":7003",
                        "redis://" + host + ":7004",
                        "redis://" + host + ":7005",
                        "redis://" + host + ":7006"
                );

        // 如果集群设置了密码，取消下面的注释
        /*
        if (password != null && !password.isEmpty()) {
            config.useClusterServers().setPassword(password);
        }
        */
        // =================================================================

        return Redisson.create(config);
    }
}