package com.hmall.trade.domain.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 传递给RocketMQ事务监听器的Lua执行上下文
 * 避免在executeLocalTransaction里重复构建keys/args
 * 新增字段 {@code itemIds}：与 KEYS[1..n] 的库存 key 一一对应，
 * 用于应用层在 Lua 返回负数时，定位到具体是哪个 itemId 的库存 key 缺失，
 * 进而调用 item-service 懒加载后单次重试。
 */
@Data
@Builder
@NoArgsConstructor          // ← 新增：生成无参构造器
@AllArgsConstructor         // ← 新增：让 @Builder 的全参构造器变 public
public class LuaExecutionContext {

    /**
     * 订单号，用于幂等、日志追踪
     */
    private String orderId;

    /**
     * Lua 脚本的 KEYS 参数：
     * [stock_key_1, ..., stock_key_n, outbox_key, epoch_key, seq_key, flag_key]
     */
    private List<String> keys;

    /**
     * Lua 脚本的 ARGV 参数：
     * [qty_1, ..., qty_n, orderId]
     */
    private Object[] args;

    /**
     * 与 KEYS[1..n] 一一对应的商品 ID 列表（长度 = n，不含 outbox/epoch/seq/flag）。
     *
     * <p>Lua 返回 -(i)（i > 0）时：
     * <pre>
     *   Long missingItemId = itemIds.get((int)(-result) - 1);
     * </pre>
     * 即可定位缺失的商品，调用 item-service 懒加载后重试。
     */
    private List<Long> itemIds;
}