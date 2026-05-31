package com.hmall.trade.controller;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.hmall.common.utils.BeanUtils;
import com.hmall.trade.domain.document.EsOrderDoc;
import com.hmall.trade.domain.po.LocalEventOutbox;
import com.hmall.trade.mapper.LocalEventOutboxMapper;
import org.springframework.validation.annotation.Validated;
import com.hmall.trade.domain.dto.OrderFormDTO;
import com.hmall.trade.domain.vo.OrderVO;
import com.hmall.trade.service.IOrderService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import lombok.RequiredArgsConstructor;
import org.apache.ibatis.annotations.Param;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Api(tags = "订单管理接口")
@Validated
@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {
    private final IOrderService orderService;
    private final LocalEventOutboxMapper localEventOutboxMapper;

    @ApiOperation("根据id查询订单")
    @GetMapping("{id}")
    public OrderVO queryOrderById(@Param ("订单id")@PathVariable("id") Long orderId) {
        return BeanUtils.copyBean(orderService.getById(orderId), OrderVO.class);
    }

    @ApiOperation("创建订单")
    @PostMapping
    public Long createOrder(@RequestBody @Validated OrderFormDTO orderFormDTO){
        return orderService.createOrder(orderFormDTO);
    }

    @ApiOperation("标记订单已支付")
    @ApiImplicitParam(name = "orderId", value = "订单id", paramType = "path")
    @PutMapping("/{orderId}")
    public void markOrderPaySuccess(@PathVariable("orderId") Long orderId) {
        orderService.markOrderPaySuccess(orderId);
    }

    @GetMapping("/pending-exists")
    public Boolean existsPendingOutbox() {
        Integer count = localEventOutboxMapper.selectCount(new LambdaQueryWrapper<LocalEventOutbox>()
                .eq(LocalEventOutbox::getStatus, 0)
                .last("LIMIT 1"));
        return count != null && count > 0;
    }
    /*@GetMapping("/user/{userId}")
    public List<EsOrderDoc> getUserOrders(
            @PathVariable("userId") Long userId,
            @RequestParam(value = "page", defaultValue = "1") int page,
            @RequestParam(value = "size", defaultValue = "10") int size) {
        return orderService.searchUserOrders(userId, page, size);
    }

    @GetMapping("/merchant/{merchantId}")
    public List<EsOrderDoc> getMerchantOrders(
            @PathVariable("merchantId") Long merchantId,
            @RequestParam(value = "page", defaultValue = "1") int page,
            @RequestParam(value = "size", defaultValue = "10") int size) {
        return orderService.searchMerchantOrders(merchantId, page, size);
    }*/
}
