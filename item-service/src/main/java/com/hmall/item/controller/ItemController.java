package com.hmall.item.controller;

import com.hmall.api.dto.ItemDTO;
import com.hmall.api.dto.OrderDetailDTO;
import com.hmall.common.domain.PageDTO;
import com.hmall.common.domain.PageQuery;
import com.hmall.common.utils.BeanUtils;
import com.hmall.item.constants.MQConstants;
import com.hmall.item.domain.po.Item;
import com.hmall.item.service.IItemService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Api(tags = "商品管理相关接口")
@RestController
@RequestMapping("/items")
@RequiredArgsConstructor
public class ItemController {

    private final RabbitTemplate rabbitTemplate;

    private final IItemService itemService;

    @ApiOperation("分页查询商品")
    @GetMapping("/page")
    public PageDTO<ItemDTO> queryItemByPage(PageQuery query) {
        // 注释掉原有的 DB 直查代码
//        // 1.分页查询
//        Page<Item> result = itemService.page(query.toMpPage("update_time", false));
//        // 2.封装并返回
//        return PageDTO.of(result, ItemDTO.class);
        // 替换为 DCL 缓存架构方法
        return itemService.queryItemByPageWithCache(query);
    }

    @ApiOperation("根据id批量查询商品")
    @GetMapping
    public List<ItemDTO> queryItemByIds(@RequestParam("ids") List<Long> ids){
        return itemService.queryItemByIds(ids);
    }

    @ApiOperation("根据id查询商品")
    @GetMapping("{id}")
    public ItemDTO queryItemById(@PathVariable("id") Long id) {
        return BeanUtils.copyBean(itemService.getById(id), ItemDTO.class);
    }

    @ApiOperation("新增商品")
    @PostMapping
    public void saveItem(@RequestBody ItemDTO item) {
        // 新增
        itemService.save(BeanUtils.copyBean(item, Item.class));
        //MQ异步通知es新增
        rabbitTemplate.convertAndSend(MQConstants.LAZY_EXCHANGE_NAME,MQConstants.LAZY_ITEM_ADD_KEY,item.getId());
    }

    @ApiOperation("更新商品状态")
    @PutMapping("/status/{id}/{status}")
    public void updateItemStatus(@PathVariable("id") Long id, @PathVariable("status") Integer status){
        Item item = new Item();
        item.setId(id);
        item.setStatus(status);
        itemService.updateById(item);
    }

    @ApiOperation("更新商品")
    @PutMapping
    public void updateItem(@RequestBody ItemDTO item) {
        // 不允许修改商品状态，所以强制设置为null，更新时，就会忽略该字段
        item.setStatus(null);
        // 更新
        itemService.updateById(BeanUtils.copyBean(item, Item.class));
        //MQ异步通知es更新商品
        rabbitTemplate.convertAndSend(MQConstants.LAZY_EXCHANGE_NAME,MQConstants.LAZY_ITEM_UPDATE_KEY,item.getId());
    }

    @ApiOperation("根据id删除商品")
    @DeleteMapping("{id}")
    public void deleteItemById(@PathVariable("id") Long id) {
        itemService.removeById(id);
        //MQ异步通知es删除对应id文档
        rabbitTemplate.convertAndSend(MQConstants.LAZY_EXCHANGE_NAME,MQConstants.LAZY_ITEM_DELETE_KEY,id);
    }

    @ApiOperation("批量扣减库存")
    @PutMapping("/stock/deduct")
    public void deductStock(@RequestParam("orderId") Long orderId, @RequestBody List<OrderDetailDTO> items){
        itemService.deductStock(orderId,items);
    }

    @ApiOperation("批量增加库存")
    @PutMapping("/stock/increase")
    public void increaseStock(@RequestParam("orderId") Long orderId, @RequestBody List<OrderDetailDTO> items){
        itemService.increaseStock(orderId,items);
    }

    /**
     * 懒加载单个商品库存到 Redis。
     *
     * <p>供 trade-service 在 Lua 扣减时发现库存 key 缺失时调用。
     * 仅内部服务调用，不对外暴露（可在网关层拦截 /items/stock/load/*）。
     *
     * @param itemId 商品 ID
     */
    @PostMapping("/stock/load/{itemId}")
    public void loadStockToRedis(@PathVariable("itemId") Long itemId) {
        itemService.loadStockToRedis(itemId);
    }

    /**
     * 批量懒加载商品库存到 Redis（供 trade-service Feign 调用）
     */
    @PostMapping("/stock/batch-load")
    public void batchLoadStockToRedis(@RequestBody List<Long> itemIds) {
        itemService.batchLoadStockToRedis(itemIds);
    }
}
