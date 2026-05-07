package com.hmall.item.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmall.item.domain.po.StockReconcileState;
import com.hmall.item.mapper.StockReconcileStateMapper;
import com.hmall.item.service.IStockReconcileStateService;
import org.springframework.stereotype.Service;

@Service
public class StockReconcileStateServiceImpl extends ServiceImpl<StockReconcileStateMapper, StockReconcileState>
        implements IStockReconcileStateService {
}
