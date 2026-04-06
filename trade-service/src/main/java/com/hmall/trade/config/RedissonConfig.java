package com.hmall.trade.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedissonConfig {

    @Value("${spring.redis.host}")
    private String host;

    @Value("${spring.redis.port}")
    private String port;

    @Value("${spring.redis.password:}") // 冒号后面为空表示默认没有密码
    private String password;

    @Bean
    public RedissonClient redissonClient() {
        Config config = new Config();
        // Redis url 必须以 redis:// 或 rediss:// (SSL) 开头
        String redisAddress = String.format("redis://%s:%s", host, port);

        // 配置单节点模式 (如果是集群，使用 useClusterServers())
        config.useSingleServer()
                .setAddress(redisAddress)
                .setDatabase(0);

        // 如果有密码则设置密码
        if (password != null && !password.isEmpty()) {
            config.useSingleServer().setPassword(password);
        }

        return Redisson.create(config);
    }
}