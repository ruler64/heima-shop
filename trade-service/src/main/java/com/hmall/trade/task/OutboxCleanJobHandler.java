package com.hmall.trade.task;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.hmall.trade.domain.po.LocalEventOutbox;
import com.hmall.trade.mapper.LocalEventOutboxMapper;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 对于MySQL中的消息表，执行自动清理与归档任务
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxCleanJobHandler {

    private final LocalEventOutboxMapper outboxMapper;
    private final JdbcTemplate jdbcTemplate;

    /**
     * 清理 7 天前的已处理数据，归档到历史表
     * 建议执行频率：每天凌晨 3:00
     */
    @XxlJob("cleanOutboxJob")
    @Transactional(rollbackFor = Exception.class) // 【关键】保证搬运和删除的原子性
    public void cleanProcessedEvents() {
        // 1. 设置清理阈值：7天前已处理的任务
        LocalDateTime thresholdTime = LocalDateTime.now().minusDays(7);
        int batchSize = 500; // 建议分批，防止大事务锁表

        // 2. 查询待清理的数据 ID
        List<LocalEventOutbox> expiredTasks = outboxMapper.selectList(
                new LambdaQueryWrapper<LocalEventOutbox>()
                        .eq(LocalEventOutbox::getStatus, 1) // 必须是已处理
                        .le(LocalEventOutbox::getUpdateTime, thresholdTime)
                        .last("LIMIT " + batchSize)
        );

        if (expiredTasks.isEmpty()) {
            XxlJobHelper.log("未发现可清理的过期数据");
            return;
        }

        List<Long> ids = expiredTasks.stream().map(LocalEventOutbox::getId).collect(Collectors.toList());

        // 3. 【归档】将数据搬运到历史表
        // 使用 INSERT IGNORE 保证幂等：如果任务重跑导致 ID 已存在，则忽略插入
        String archiveSql = "INSERT IGNORE INTO local_event_outbox_history " +
                "SELECT * FROM local_event_outbox WHERE id IN (" +
                ids.stream().map(Object::toString).collect(Collectors.joining(",")) + ")";

        jdbcTemplate.execute(archiveSql);
        XxlJobHelper.log("成功归档 {} 条数据至历史表", ids.size());

        // 4. 【清理】从主表删除
        outboxMapper.deleteBatchIds(ids);
        XxlJobHelper.log("成功从主表清理 {} 条数据", ids.size());

        log.info("Outbox 表清理完成，批次大小: {}", ids.size());
    }
    /*
    打法一：最暴力的优雅 —— 明确业务底线（TTL机制）
    首先我们要问产品经理和业务方一个问题：“对于一个已经成功取消的订单，它几个月前的系统内部 MQ 补偿流水，真的有价值吗？”
    答案通常是：没有价值。 这种数据仅仅用于研发在一周内排查线上 Bug，最多保留 1~3 个月。

    解法：
    在原有的 XXL-JOB 清理任务里，再加一个“物理删除”的步骤。
    每天凌晨，不仅把 7 天前的数据从“主表”搬到“历史表”，同时执行一条 SQL，把历史表里 3 个月前的数据彻底硬删除（Hard Delete）。依然是分批删，不留一点历史包袱。

    打法二：DBA 的终极杀器 —— MySQL 表分区（Partitioning）
    如果业务方非要说：“不行，财务要求对账日志必须保留 1 年！”
    如果用普通的 DELETE 语句去删除 1 年前、动辄几千万行的数据，会产生巨大的 Undo Log，严重消耗 MySQL 性能甚至锁表。

    这时候，真正的高级架构师会采用 MySQL 表分区（Range Partition）。

    我们在创建 local_event_outbox_history 表时，就按“月”给它分好物理区：

    SQL
    -- 创建按月分区的历史表
    CREATE TABLE `local_event_outbox_history` (
      `id` bigint(20) NOT NULL,
      `event_type` varchar(255) DEFAULT NULL,
      `payload` text,
      `status` int(11) DEFAULT NULL,
      `create_time` datetime NOT NULL, -- 必须是非空，作为分区键
      `update_time` datetime DEFAULT NULL,
      PRIMARY KEY (`id`, `create_time`) -- 分区键必须包含在主键中
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    PARTITION BY RANGE (YEAR(create_time) * 100 + MONTH(create_time)) (
        PARTITION p202311 VALUES LESS THAN (202312),
        PARTITION p202312 VALUES LESS THAN (202401),
        PARTITION p202401 VALUES LESS THAN (202402),
        PARTITION p_max VALUES LESS THAN MAXVALUE
    );
    为什么这招是绝杀？
    当你需要清理 1 年前（比如 2023年11月）的数据时，你不需要执行任何 DELETE 语句，只需要让 DBA 跑一条指令：

    SQL
    ALTER TABLE local_event_outbox_history DROP PARTITION p202311;
    这条指令的时间复杂度是 O(1)，直接在操作系统层面把那个月的底层数据文件（.ibd）给删了！ 瞬间释放几十 G 磁盘空间，对线上业务的影响几乎为 0，这叫“秒级清理”。

    打法三：冷热分离终极架构 —— 大数据离线数仓（Data Lake）
    如果你遇到的是蚂蚁金服级别的要求：“所有流水永久保存，供国家监管审计！”

    这个时候，关系型数据库 MySQL 已经不适合承担这个存储任务了。
    解法：

    历史表仅仅保留最近 1~3 个月的数据（热数据/温数据），用于快速排查。

    引入大数据的同步工具（如 DataX 或 Canal + Kafka），每天晚上把昨天的数据同步到大数据平台的冷存储中（如 HDFS、Hive、阿里云 OSS、或者 ClickHouse）。

    同步成功后，MySQL 里的历史数据就可以被彻底清空了。大数据平台存储成本极其低廉，存 100 年都不是问题。
     */
}