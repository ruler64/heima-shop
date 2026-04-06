package com.hmall.trade.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.hmall.api.dto.OrderDetailDTO;
import com.hmall.trade.domain.po.LocalEventOutbox;
import com.hmall.trade.mapper.LocalEventOutboxMapper;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * XXL-JOB 兜底补偿任务：扫描消息表，执行 Redis 退库---已弃用！！！trade不应插手item商品对于缓存的管理，
 * 只在ItemRestoreJobHandler发消息给他，让他来通过canal处理即可
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RedisRestoreJobHandler {

    private final LocalEventOutboxMapper outboxMapper;
    private final StringRedisTemplate stringRedisTemplate;

    // 依然保留我们的防重放 Lua 神器
    private static final String RESTORE_STOCK_LUA =
            "if redis.call('setnx', KEYS[1], '1') == 1 then " +
                    "   redis.call('expire', KEYS[1], 86400) " +  // 防重放凭证 24 小时有效
                    "   for i = 2, #KEYS do " +
                    "       redis.call('incrby', KEYS[i], ARGV[i-1]) " +
                    "   end " +
                    "   return 1 " +
                    "else " +
                    "   return 0 " +
                    "end";

    private static final DefaultRedisScript<Long> RESTORE_SCRIPT = new DefaultRedisScript<>(RESTORE_STOCK_LUA, Long.class);

    @XxlJob("restoreRedisStockJob")
    public void restoreRedisStock() {
        XxlJobHelper.log("定时退库补偿任务启动...");

        // 1. 捞取待处理的退库任务 (每次最多处理 100 条，防内存溢出)
        List<LocalEventOutbox> tasks = outboxMapper.selectList(
                new LambdaQueryWrapper<LocalEventOutbox>()
                        .eq(LocalEventOutbox::getEventType, "RESTORE_REDIS_STOCK")
                        .eq(LocalEventOutbox::getStatus, 0)
                        .last("LIMIT 100")
        );

        if (tasks == null || tasks.isEmpty()) {
            return;
        }

        for (LocalEventOutbox task : tasks) {
            try {
                // 2. 解析 payload 数据
                JSONObject payload = JSON.parseObject(task.getPayload());
                Long orderId = payload.getLong("orderId");
                List<OrderDetailDTO> details = payload.getJSONArray("details").toJavaList(OrderDetailDTO.class);

                // 3. 准备 Lua 参数
                List<String> keys = new ArrayList<>();
                keys.add("order:restore:idempotent:" + orderId);
                Object[] args = new Object[details.size()];

                for (int i = 0; i < details.size(); i++) {
                    keys.add("item:stock:" + details.get(i).getItemId());
                    args[i] = String.valueOf(details.get(i).getNum());
                }

                // 4. 执行 Redis 原子退库
                Long result = stringRedisTemplate.execute(RESTORE_SCRIPT, keys, args);

                if (result != null && result == 1L) {
                    XxlJobHelper.log("订单 {} 定时兜底 Redis 退库成功！", orderId);
                } else {
                    XxlJobHelper.log("订单 {} Redis 触发幂等（之前可能已退过）", orderId);
                }

                // 5. 【关键闭环】无论成功还是幂等，只要没抛异常，都说明该任务使命结束！改状态为已处理
                task.setStatus(1);
                task.setUpdateTime(LocalDateTime.now());
                outboxMapper.updateById(task);

            } catch (Exception e) {
                // 如果 Redis 刚好连不上，这里会捕获异常。
                // 此时不对该 task 的 status 做任何修改，它依旧是 0。
                // 等下一个定时任务周期，它会被重新捞出来执行，直到成功！
                XxlJobHelper.log("处理 Redis 退库遇到异常，等待下次重试。任务ID: " + task.getId());
                log.error("Redis 补偿任务异常", e);
            }
        }
    }
}