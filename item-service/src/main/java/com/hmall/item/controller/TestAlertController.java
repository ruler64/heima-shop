package com.hmall.item.controller;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
@RestController
@RequestMapping("/actuator/test")
@RequiredArgsConstructor
public class TestAlertController {

    // 🌟 注入 Spring Boot 自带的 MeterRegistry，用来向 Prometheus 发射自定义埋点
    private final MeterRegistry meterRegistry;

    // 🌟 核心武器：用一个强引用列表持有内存，防止垃圾回收（GC）将其回收
    private final List<byte[]> memoryLeaker = new CopyOnWriteArrayList<>();

    /**
     * 触发【库存数据异常】告警 (级别: critical)
     * 访问路径：http://192.168.31.1:8081/actuator/test/anomaly?itemId=999
     */
    @GetMapping("/anomaly")
    public String triggerStockAnomaly(@RequestParam(defaultValue = "88888") Long itemId) {
        log.warn("🚨 [测试] 正在手动触发库存异常埋点，商品ID: {}", itemId);

        // 🌟 这里的名字必须和 alert_rules.yml 中的 expr: increase(stock_reconciliation_anomaly_total[5m]) 完全对应
        // 🌟 这里的 Tag 必须包含 itemId 和 type，因为你的告警描述里用到了 {{ $labels.itemId }} 和 {{ $labels.type }}
        meterRegistry.counter("stock_reconciliation_anomaly_total",
                "itemId", String.valueOf(itemId),
                "type", "MANUAL_TEST_CRITICAL"
        ).increment();

        return "【成功】已向 Prometheus 发射一条库存异常指标(itemId=" + itemId + ")！请静候钉钉告警通知。";
    }

    /**
     * 触发【Epoch已修复】警告 (级别: warning)
     * 访问路径：http://192.168.31.1:8081/actuator/test/repair?itemId=999
     */
    @GetMapping("/repair")
    public String triggerStockRepair(@RequestParam(defaultValue = "66666") Long itemId) {
        log.info("⚠️ [测试] 正在手动触发Epoch修复埋点，商品ID: {}", itemId);

        // 对应 alert_rules.yml 中的 increase(stock_reconciliation_repair_total[10m])
        meterRegistry.counter("stock_reconciliation_repair_total",
                "itemId", String.valueOf(itemId)
        ).increment();

        return "【成功】已向 Prometheus 发射一条Epoch修复指标(itemId=" + itemId + ")！由于是warning级别，Alertmanager会等待10秒进行聚合。";
    }

    /**
     * 🔥 真实触发【JVM内存危机】告警 (安全、可控、真实)
     * 访问路径：http://localhost:8081/actuator/test/jvm
     */
    @GetMapping("/jvm")
    public String triggerJvm() {
        log.warn("🚨 [测试] 正在模拟真实的 JVM 内存暴涨...");

        // 每次请求，在堆内存中死死锁住 50MB 的空间
        byte[] block = new byte[50 * 1024 * 1024]; // 50MB
        memoryLeaker.add(block);

        // 获取当前 JVM 内存状态
        long freeMemory = Runtime.getRuntime().freeMemory() / 1024 / 1024;
        long totalMemory = Runtime.getRuntime().totalMemory() / 1024 / 1024;
        long maxMemory = Runtime.getRuntime().maxMemory() / 1024 / 1024;

        return String.format("【成功】已在堆中锁死 50MB 内存！<br/>" +
                        "当前泄露块数: %d<br/>" +
                        "JVM空闲内存: %d MB / 总分配内存: %d MB / 最大可用内存: %d MB。<br/>" +
                        "💡 提示：请连续狂点此接口数次，把内存顶上去触发真实告警！测试完后务必访问 /jvm/clean 释放内存！",
                memoryLeaker.size(), freeMemory, totalMemory, maxMemory);
    }

    /**
     * 🧹 一键拯救 JVM：释放内存，防止微服务真的 OOM 挂掉
     * 访问路径：http://localhost:8081/actuator/test/jvm/clean
     */
    @GetMapping("/jvm/clean")
    public String cleanJvm() {
        log.info("🧹 [测试] 正在释放模拟的 JVM 泄露内存，解救微服务...");

        int removedBlocks = memoryLeaker.size();
        memoryLeaker.clear(); // 斩断强引用

        System.gc(); // 强制提醒 JVM 赶紧进行垃圾回收

        return "【成功】已清空测试内存（共释放 " + removedBlocks + " 块），并触发了 System.gc()。请静候 Prometheus 指标回落。";
    }
}