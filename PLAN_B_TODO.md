# PLAN_B_TODO

## 目标

把“创建订单”和“库存扣减”放到 `trade-service` 的**同一个 MySQL 本地事务**中完成强一致（跨服务最终一致通过 MQ/outbox 兜底）。

方案 B 的核心变化：
1. 库存的“强一致写模型”从 `item-service` / Redis Lua 前置，迁移到 `trade-service`
2. `item-service` 只负责“读模型/缓存/搜索索引/最终一致的库存镜像更新”
3. 下单链路不再依赖 Redis Lua 的多 key 原子性（避免 Redis Cluster 多 slot 限制成为致命约束）

---

## 新增/调整的核心表结构（建议在 `trade-service` 数据库内新增）

> 下文列名以业务最小闭环为目标，具体字段可按你现有表风格微调。

### 1）`trade_item_stock`（库存强一致写模型）

用途：`trade-service` 本地事务中扣减/回补的“最终可用库存”。

- `item_id` bigint NOT NULL
- `stock` int NOT NULL
- `version` bigint NOT NULL DEFAULT 0（可选：用于乐观锁，提升并发冲突可读性）
- `update_time` datetime NOT NULL

约束/索引：
- PRIMARY KEY(`item_id`)

更新 SQL（强一致扣减）建议形态（任选其一）：
- 模式 A：`UPDATE trade_item_stock SET stock = stock - :qty WHERE item_id = :itemId AND stock >= :qty`
- 模式 B：`UPDATE ... SET stock = stock - :qty, version = version+1 WHERE item_id = :itemId AND stock >= :qty AND version = :ver`

### 2）`trade_stock_deduct_log`（库存扣减幂等 + 状态机）

用途：保证同一个 `orderId` 重放时不会重复扣减；并在回滚/关闭时只允许“从已扣减->已回补”流转一次。

- `order_id` bigint NOT NULL
- `status` tinyint NOT NULL（1=已扣减，2=已回补/已恢复）
- `create_time` datetime NOT NULL
- `update_time` datetime NOT NULL
- `biz_tag` varchar(64) NULL（可选：用于扩展更多业务类型）

约束/索引：
- PRIMARY KEY(`order_id`) 或 UNIQUE(`order_id`)
- 可选：`INDEX(status)`

幂等状态机 SQL 示例：
- 插入扣减流水：`INSERT ... (order_id,status=1,...)`（依赖唯一约束；重复直接幂等放行）
- 回补流水状态机：`UPDATE ... SET status=2,update_time=now() WHERE order_id=:orderId AND status=1`

### 3）事件通知 Outbox（复用你现有 `LocalEventOutbox` 或单独新增）

你现有 `trade-service` 已有 `LocalEventOutbox`，目前用于：
- `ORDER_CREATED`（通知 item/cart）
- `RESTORE_ITEM_STOCK`（通知 item 恢复库存）

方案 B 中建议继续复用，但确保：
- `LocalEventOutbox` 的写入与“订单/库存写模型更新”处于同一个事务内
- outbox 状态字段与定时任务重投递逻辑保持一致

如当前 `LocalEventOutbox` 无法满足扩展（例如需区分“创建命令”与“结果事件”），可新增一张：

#### 3.1）`trade_create_order_command_outbox`

用途：如果你仍保留“下单请求->MQ异步落库”的模式，需保证“命令投递不丢”。命令入库后再由后台/afterCommit投递 MQ。

字段建议：
- `id` bigint PK
- `order_id` bigint NOT NULL
- `payload` text NOT NULL
- `status` tinyint NOT NULL（0=待发送，1=已发送，2=失败/超时）
- `retry_count` int NOT NULL
- `create_time` datetime NOT NULL
- `update_time` datetime NOT NULL

---

## 重构蓝图（按链路拆解）

### A）下单（强一致：订单创建 + 库存扣减同事务）

建议落地点：仍保留你的 MQ 消费者落库函数（`OrderMQListener -> OrderServiceImpl.handleDbOrder`），把库存扣减逻辑也放进该 `@Transactional` 方法里。

关键流程（同一个事务）：
1. 幂等兜底：检查 `order` 主键是否存在（或直接依赖唯一索引异常）
2. 查询商品价格/信息：调用 `item-service` 的只读接口（不做库存扣减）
3. 写入 `order` + `order_detail`
4. 对每个 item 执行强一致扣减：
   - 插入 `trade_stock_deduct_log(orderId,status=1)`（唯一约束防重放；可选：先插 log 再扣库存）
   - 执行 `trade_item_stock` 批量扣减（要求 affectedRows 与明细数量一致，否则回滚抛异常）
5. 写入 `LocalEventOutbox` 事件（`ORDER_CREATED`，供 item/cart 异步更新读模型）
6. 事务提交成功后，由 outbox 发布任务把事件投递 MQ

结果：
- trade 数据库内保证“创建订单成功 <=> 库存扣减成功”
- 由于 item-service 是异步更新，不允许用 item-service 库存做强一致判定

### B）取消/超时关单（强一致：关闭订单 + 库存回补同事务）

建议落地点：`cancelOrder` / `cancelOrderAndRestore`（都在 `trade-service` 内）

关键流程（同一个事务）：
1. 更新订单状态为 `CLOSED`（只允许 `UNPAID` 才能关闭，依赖状态机 SQL）
2. 幂等回补：
   - 更新 `trade_stock_deduct_log` 状态从 1->2（受影响行数为 0 则幂等放行）
3. 根据 `order_detail` 批量回补 `trade_item_stock`（`stock = stock + qty`）
4. 写入 `LocalEventOutbox` 事件 `RESTORE_ITEM_STOCK`（通知 item-service 恢复其库存镜像与 Redis 缓存）

### C）支付成功（不改库存，但要改订单状态）

保持你现有 `markOrderPaySuccess` 逻辑即可：
- 订单状态机：`UNPAID -> PAID`
- 不需要库存变化（库存已在创建订单时扣减，未支付关闭时才回补）

### D）item-service 侧改造（从“库存强一致扣减方”变为“库存镜像同步方”）

1. 保留监听器（例如 `ItemOrderDeductListener`、`ItemRestoreListener`）
2. 监听到 `ORDER_CREATED`：
   - 执行 item-service 自己的库存扣减（作为读模型一致性补偿）
   - 使用你现有 `StockDeductLog` 幂等
3. 监听到 `RESTORE_ITEM_STOCK`：
   - 执行 item-service 自己的库存回补（作为读模型一致性补偿）
   - 使用现有 `status` 状态机幂等

注意：
- item-service 的库存扣减成功与否不再影响 trade 强一致正确性
- item-service 若失败，会通过 MQ 重试最终追平（你已有幂等兜底）

### E）Redis 的角色调整

方案 B 不再把 Redis Lua 当成强一致原子预扣核心。

建议：
1. Redis 可继续用于：
   - 缓存商品详情分页（你已有）
   - 缓存库存镜像（由 item-service 在扣减/回补后更新，或由对账任务覆盖）
2. Redis 不做“多 key 原子扣减”强一致预扣（避免 Lua 跨 slot）

---

## 你需要在代码层面重点改的点（TODO 清单）

1. `trade-service` 新增 `trade_item_stock`、`trade_stock_deduct_log` 的实体类/Mapper/SQL
2. 把库存扣减从 item-service RPC/监听路径，迁移到 `trade-service` 的本地事务中（`handleDbOrder`）
3. 把取消回补的逻辑从 item-service RPC，迁移到 `trade-service` 本地事务（`cancelOrder`）
4. outbox 写入时机调整：确保 outbox 入库与“订单+库存强一致写模型更新”同事务
5. MQ 消息 payload 统一（DTO 化），保证 item-service 能正确执行扣减/回补
6. item-service 保持幂等即可（`StockDeductLog` 唯一约束/状态机逻辑继续复用）
7. Redis 库存 key 的更新策略改为“由 item-service 最终一致更新 + 对账覆盖”

