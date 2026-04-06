package com.hmall.item.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.hmall.api.dto.OrderDetailDTO;
import com.hmall.item.domain.po.Item;
import org.apache.ibatis.annotations.Update;
import org.springframework.data.repository.query.Param;

import java.util.List;

/**
 * <p>
 * 商品表 Mapper 接口
 * </p>
 *
 * @author 虎哥
 * @since 2023-05-05
 */
public interface ItemMapper extends BaseMapper<Item> {

    @Update("UPDATE item SET stock = stock - #{num} WHERE id = #{itemId} AND stock >= #{num}")
    void updateStock(OrderDetailDTO orderDetail);

    // 【大厂细节】利用 WHERE stock >= num 作为乐观锁防线，数据库天然具备行级锁，绝对不会超卖
    @Update("UPDATE item SET stock = stock - #{num} WHERE id = #{itemId} AND stock >= #{num}")
    int deductStockSafe(@Param("itemId") Long itemId, @Param("num") Integer num);

    // 【新增】安全加库存：直接在数据库层面利用原子操作累加，彻底消灭内存覆盖漏洞
    @Update("UPDATE item SET stock = stock + #{num} WHERE id = #{itemId}")
    int increaseStockSafe(@Param("itemId") Long itemId, @Param("num") Integer num);

    //从for循环更改MySQL库存-》演进为批量更改
    // 批量安全扣减
    int batchDeductStockSafe(@Param("list") List<OrderDetailDTO> list);

    // 批量安全恢复
    int batchIncreaseStockSafe(@Param("list") List<OrderDetailDTO> list);
}
