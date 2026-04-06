package com.hmall.trade.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@EnableTransactionManagement // 确保事务管理机制全面生效
public class TransactionConfig {
    // 默认 Spring Boot 已配置 DataSourceTransactionManager，这里仅作开启声明
}