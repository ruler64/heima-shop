package com.hmall.trade.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;

@Configuration
public class RedisConfig {

    // 提前将 预扣减+保存消息 的复合 Lua 脚本加载进 Spring 内存
    @Bean("deductStockAndSaveMsgScript")
    public DefaultRedisScript<Long> deductStockAndSaveMsgScript() {
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
        // 确保你的 src/main/resources/lua/batch_deduct_stock.lua文件存在
        redisScript.setLocation(new ClassPathResource("lua/batch_deduct_stock.lua"));
        redisScript.setResultType(Long.class);
        return redisScript;
    }
}