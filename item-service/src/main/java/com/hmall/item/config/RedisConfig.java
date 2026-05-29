package com.hmall.item.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;

@Configuration
public class RedisConfig {

    // 提前将 预扣减+保存消息 的复合 Lua 脚本加载进 Spring 内存
    @Bean("stockDeductWithVersion")
    public DefaultRedisScript<Long> stockDeductWithVersion() {
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
        // 确保你的 src/main/resources/lua/batch_deduct_stock.lua文件存在
        redisScript.setLocation(new ClassPathResource("lua/stock_deduct_with_version.lua"));
        redisScript.setResultType(Long.class);
        return redisScript;
    }
    @Bean("stockRestoreWithVersion")
    public DefaultRedisScript<Long> stockRestoreWithVersion() {
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
        // 确保你的 src/main/resources/lua/batch_deduct_stock.lua文件存在
        redisScript.setLocation(new ClassPathResource("lua/stock_restore_with_version.lua"));
        redisScript.setResultType(Long.class);
        return redisScript;
    }

    /**
     * 库存批量恢复LUA脚本
     * @return
     */
    @Bean("restoreRedisStock")
    public DefaultRedisScript<Long> restoreRedisStock() {
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
        // 确保你的 src/main/resources/lua/batch_deduct_stock.lua文件存在
        redisScript.setLocation(new ClassPathResource("lua/restore_redis_stock.lua"));
        redisScript.setResultType(Long.class);
        return redisScript;
    }

    /**
     * 商品库存与版本号，懒加载LUA脚本初始化
     * @return
     */
    @Bean("batchLazyLoadScript")
    public DefaultRedisScript<Long> batchLazyLoadScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("lua/batch_lazy_load_stock.lua"));
        script.setResultType(Long.class);
        return script;
    }

    /**
     * 商品库存与版本号、流水号，预热LUA脚本初始化
     * @return
     */
    @Bean("batchPreloadStockScript")
    public DefaultRedisScript<Long> batchPreloadStock() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("lua/batch_preload_stock.lua"));
        script.setResultType(Long.class);
        return script;
    }
    /**
     * 商品库存与版本号、流水号，预热LUA脚本初始化
     * @return
     */
    @Bean("failoverEpochUpgrade")
    public DefaultRedisScript<Long> failoverEpochUpgrade() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("lua/failover_epoch_upgrade.lua"));
        script.setResultType(Long.class);
        return script;
    }
}