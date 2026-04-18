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
     * 事件类型，例如：ORDER_CREATED
     */
    private String eventType;

    /**
     * 事件内容 (JSON 字符串)
     */
    private String payload;

    /**
     * 状态：0-待发送，1-已发送
     */
    private Integer status;

    @TableField("create_time")
    private LocalDateTime createTime;

    @TableField("update_time")
    private LocalDateTime updateTime;
}