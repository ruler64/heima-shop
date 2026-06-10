package com.hmall.trade.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
@EnableTransactionManagement // 确保事务管理机制全面生效
public class TransactionConfig {
    // 默认 Spring Boot 已配置 DataSourceTransactionManager，这里仅作开启声明

    // ── 1. 配置类：新增专用小线程池 ───────────────────────────────────────────
    // 放在任意 @Configuration 类里（如 ThreadPoolConfig.java）
    // 职责单一：只用于 afterCommit 的 outbox 状态回写，与业务线程池隔离
    @Bean("outboxStatusExecutor")
    public Executor outboxStatusExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(2);         // afterCommit 回写并发量极低，2条常驻即可
        executor.setMaxPoolSize(4);
        executor.setQueueCapacity(1000);     // 积压时缓冲，不丢弃
        // 2. 🌟 显式指定空闲线程回收时间（按秒计）
        executor.setKeepAliveSeconds(30);    // 空闲 30 秒自动回收

        // 3. 🌟 允许核心线程也超时回收（核心优化）
        // 这样在深夜没有交易时，整个线程池持有的线程数会安全降为 0，不占用任何系统资源
        executor.setAllowCoreThreadTimeOut(true);

        executor.setThreadNamePrefix("outbox-status-");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
        return executor;
    }
}