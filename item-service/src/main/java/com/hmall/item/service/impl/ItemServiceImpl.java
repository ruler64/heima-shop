package com.hmall.item.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmall.api.dto.ItemDTO;
import com.hmall.api.dto.OrderDetailDTO;
import com.hmall.common.exception.BizIllegalException;
import com.hmall.common.utils.BeanUtils;
import com.hmall.item.domain.po.Item;
import com.hmall.item.mapper.ItemMapper;
import com.hmall.item.service.IItemService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collection;
import java.util.List;

/**
 * <p>
 * 商品表 服务实现类
 * </p>
 *
 * @author 虎哥
 */
@Service
public class ItemServiceImpl extends ServiceImpl<ItemMapper, Item> implements IItemService {

    @Override
    @Transactional
    public void deductStock(List<OrderDetailDTO> items) {
//        String sqlStatement = "com.hmall.item.mapper.ItemMapper.updateStock";
//        boolean r = false;
//        try {
//            r = executeBatch(items, (sqlSession, entity) -> sqlSession.update(sqlStatement, entity));
//        } catch (Exception e) {
//            log.error("更新库存异常", e);
//            return;
//        }
        for (OrderDetailDTO item : items) {
            Item item1 = baseMapper.selectById(item.getItemId());
            if (item1.getStock()<item.getNum()){
                throw new BizIllegalException("库存不足！");
            }
            baseMapper.updateStock(item);
        }
//        if (!r) {
//            throw new BizIllegalException("库存不足！");
//        }
    }

    @Override
    public List<ItemDTO> queryItemByIds(Collection<Long> ids) {
        return BeanUtils.copyList(listByIds(ids), ItemDTO.class);
    }

    @Override
    public void increaseStock(List<OrderDetailDTO> orderDetailDTOS) {
        for (OrderDetailDTO orderDetailDTO : orderDetailDTOS) {
            Item item = baseMapper.selectById(orderDetailDTO.getItemId());
            item.setStock(item.getStock()+orderDetailDTO.getNum());
            baseMapper.updateById(item);
        }
    }
}
