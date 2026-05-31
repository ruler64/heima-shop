package com.hmall.trade.domain.document;

import com.hmall.trade.domain.po.Order;
import com.hmall.trade.domain.po.OrderDetail;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.beans.BeanUtils;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

@Data
@NoArgsConstructor
public class EsOrderDoc implements Serializable {
    
    private Long id;
    private Integer totalFee;
    private Integer paymentType;
    private Long userId;
    private Integer status;
    private LocalDateTime createTime; // 建议在ES中存毫秒时间戳或标准的格式化字符串
    private LocalDateTime payTime;
    
    // 嵌套的订单详情列表
    private List<OrderDetail> details;

    /**
     * 组装 ES 文档的构造函数
     */
    public EsOrderDoc(Order order, List<OrderDetail> details) {
        // 将 Order 的同名属性拷贝过来 (注意 LocalDateTime 需要单独处理格式化，这里假设简化处理)
        BeanUtils.copyProperties(order, this);
        // 2. 🌟 手动处理时间转换：LocalDateTime 转换为 Long 毫秒时间戳
        /*if (order.getCreateTime() != null) {
            this.createTime = order.getCreateTime().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        }
        if (order.getPayTime() != null) {
            this.payTime = order.getPayTime().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        }*/
        this.details = details;
    }
}