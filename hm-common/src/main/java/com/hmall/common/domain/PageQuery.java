package com.hmall.common.domain;

import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.metadata.OrderItem;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.validation.constraints.Min;

@Data
@ApiModel(description = "分页查询条件")
@Accessors(chain = true)
public class PageQuery {
    public static final Integer DEFAULT_PAGE_SIZE = 20;
    public static final Integer DEFAULT_PAGE_NUM = 1;
    @ApiModelProperty("页码")
    @Min(value = 1, message = "页码不能小于1")
    private Integer pageNo = DEFAULT_PAGE_NUM;
    @ApiModelProperty("每页数据大小")
    @Min(value = 1, message = "每页查询数量不能小于1")
    private Integer pageSize = DEFAULT_PAGE_SIZE;
    @ApiModelProperty("是否升序")
    private Boolean isAsc = true;
    @ApiModelProperty("排序方式")
    private String sortBy;

    public int from(){
        return (pageNo - 1) * pageSize;
    }

    public <T> Page<T> toMpPage(OrderItem... orderItems) {//给定排序字段，进行分页查询
        Page<T> page = new Page<>(pageNo, pageSize);
        // 如果后端硬性规定了排序方式（通过参数传进来的 orderItems）是否手动指定排序方式
        if (orderItems != null && orderItems.length > 0) {
            for (OrderItem orderItem : orderItems) {
                page.addOrder(orderItem);
            }
            return page;
        }
        // 如果后端没规定，但前端传了排序字段（比如前端要求按价格 sortBy="price"）
        if (StrUtil.isNotEmpty(sortBy)){
            OrderItem orderItem = new OrderItem();//OrderItem排序指令。它告诉框架最终的 SQL 要拼上 ORDER BY column ASC/DESC
            orderItem.setAsc(isAsc);
            orderItem.setColumn(sortBy);
            page.addOrder(orderItem);
        }
        return page;//把Page<T>对象传给 Mapper 接口，MyBatis-Plus 就会在底层自动帮你拦截 SQL
    }

    public <T> Page<T> toMpPage(String defaultSortBy, boolean isAsc) {
        if (StringUtils.isBlank(sortBy)){
            sortBy = defaultSortBy;
            this.isAsc = isAsc;
        }
        Page<T> page = new Page<>(pageNo, pageSize);
        OrderItem orderItem = new OrderItem();
        orderItem.setAsc(this.isAsc);
        orderItem.setColumn(sortBy);
        page.addOrder(orderItem);
        return page;
    }
    //默认排序字段，进行分页查询
    public <T> Page<T> toMpPageDefaultSortByCreateTimeDesc() {
        return toMpPage("create_time", false);
    }
    public <T> Page<T> toMpPageDefaultSortByUpdateTimeDesc() {
        return toMpPage("update_time", false);
    }
}
