package com.hmall.trade.domain.po;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("local_event_outbox")
public class LocalEventOutbox {

    @TableId(type = IdType.AUTO)
    private Long id;

    /**
     * 业务主键，例如 orderId
     */
    private Long orderId;

    /**
     * 事件类型，例如：ORDER_CREATED / RESTORE_ITEM_STOCK
     */
    private String eventType;

    /**
     * 事件内容 (JSON 字符串)
     */
    private String payload;

    /**
     * Redis 故障世代号
     */
    private Long epoch;

    /**
     * 当前世代内单调递增序号
     */
    private Long seq;

    /**
     * 逻辑版本号，建议格式：epoch|seq
     */
    private String version;

    /**
     * 消息来源：REDIS / MYSQL
     */
    private String source;

    /**
     * 状态：0-待发送，1-已发送，2-发送失败待重试
     */
    private Integer status;

    /**
     * 重试次数
     */
    private Integer retryCount;

    /**
     * 下次重试时间
     */
    @TableField("next_retry_time")
    private LocalDateTime nextRetryTime;

    @TableField("create_time")
    private LocalDateTime createTime;

    @TableField("update_time")
    private LocalDateTime updateTime;
}