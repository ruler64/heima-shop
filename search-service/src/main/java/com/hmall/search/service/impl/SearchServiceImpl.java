package com.hmall.search.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.core.metadata.OrderItem;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmall.api.client.TradeClient;
import com.hmall.api.dto.EsOrderDetailDTO;
import com.hmall.api.dto.OrderDTO;
import com.hmall.search.domain.doc.OrderDoc;
import com.hmall.search.domain.po.EsOrderDoc;
import com.hmall.search.domain.po.Item;
import com.hmall.search.domain.po.ItemDoc;
import com.hmall.search.domain.query.ItemPageQuery;
import com.hmall.search.mapper.SearchMapper;
import com.hmall.search.service.ISearchService;
import com.hmall.api.dto.ItemDTO;
import com.hmall.common.domain.PageDTO;
import com.hmall.common.utils.BeanUtils;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * es搜索 服务实现类
 * </p>
 *
 * @author 虎哥
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class SearchServiceImpl extends ServiceImpl<SearchMapper, Item> implements ISearchService {

    /**
     * ✅ 修正点：通过构造注入，不在字段处 new
     *    Bean 来源：ElasticSearchConfig（含超时配置 + FailureListener）
     *    @RequiredArgsConstructor 自动生成构造函数注入所有无初始值的 final 字段
     */
    private final RestHighLevelClient client;
    /**
     * Feign 降级客户端（order 查询接口待 trade-service 开放后使用）
     * 目前 TradeClient 中 order 相关接口处于注释状态，fallback 暂返回空页
     */
    private final TradeClient tradeClient; // 注入兜底 Feign 客户端

    /** Micrometer：ES 降级监控打点 */
    private final MeterRegistry meterRegistry;

    private static final String ORDER_INDEX = "order_index";

    /**
     * ✅ C端用户查自己订单 (带 Routing，只打向 1 个 Shard)
     */
    public  PageDTO<OrderDTO> searchUserOrders(Long userId, int page, int size) {
        try {
            SearchRequest request = new SearchRequest(ORDER_INDEX);

            // 🌟 核心：指定 routing，只打向存储该 userId 数据的单个 Shard
            request.routing(userId.toString());

            SearchSourceBuilder ssb = new SearchSourceBuilder();
            // userId 是 keyword 类型，用 termQuery 精确匹配（不分词）
            ssb.query(QueryBuilders.termQuery("userId", userId.toString()));
            ssb.from((page - 1) * size).size(size);
            ssb.sort("createTime", SortOrder.DESC);
            request.source(ssb);

            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            return  parseOrderPage(response, page, size);

        } catch (Exception e) {
            log.error("[ES降级] C端用户订单查询失败，触发降级。userId={}", userId, e);

            // Micrometer 打点：Prometheus 采集后可配置 Alertmanager 告警
            meterRegistry.counter("es_order_fallback_total",
                    "scene", "user_query").increment();

            // ── 降级策略 ────────────────────────────────────────────────
            // 方案：通过 Feign 调 trade-service 走 MySQL 从库兜底
            // 前提：需要在 TradeClient 开放以下接口（目前注释状态）：
            //   @GetMapping("/orders/user/{userId}")
            //   List getOrdersByUserId(@PathVariable Long userId,
            //                                    @RequestParam int page,
            //                                    @RequestParam int size);
            //
            // 开放后取消注释：
            // List fallbackDocs = tradeClient.getOrdersByUserId(userId, page, size);
            // return PageDTO.of(fallbackDocs, (long) fallbackDocs.size(), page, size);
            // ────────────────────────────────────────────────────────────

            // 暂返回空页，前端感知后提示"数据加载失败，请稍后重试"
            return PageDTO.empty((long)0, (long)0);
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // B 端：商户查所有订单（merchantId 字段暂未实现，保留完整流程供参考）
    // ─────────────────────────────────────────────────────────────────────

    /**
     * B 端商户查询名下所有订单
     *
     * ⚠ 当前状态：OrderDoc 中尚无 merchantId 字段，ES Mapping 也未包含。
     *   完整流程已注释保留，实现路径：
     *   1. Order 表增加 merchant_id 列（或从 item 维度关联）
     *   2. OrderDoc / order_index mapping 追加 merchantId（keyword）
     *   3. OrderCanalEsListener.handleOrderInsert 写入 merchantId
     *   4. 取消本方法注释
     *
     * 注意：商户查询【不能带 routing】，因为商户的订单分散在不同用户的 Shard 中，
     * 必须广播到所有 Shard 后合并结果（scatter-gather）。
     * 性能取舍：单次查询 cost 高于 C 端，但商户场景 QPS 远低于 C 端，可接受。
     */
    // @Override
    // public PageDTO searchMerchantOrders(Long merchantId, int page, int size) {
    //     try {
    //         SearchRequest request = new SearchRequest(ORDER_INDEX);
    //         // 🚨 注意：不带 routing，必须广播到全部 Shard（scatter-gather）
    //         // request.routing(...) 故意不设置
    //
    //         SearchSourceBuilder ssb = new SearchSourceBuilder();
    //         ssb.query(QueryBuilders.termQuery("merchantId", merchantId.toString()));
    //         ssb.from((page - 1) * size).size(size);
    //         ssb.sort("createTime", SortOrder.DESC);
    //         request.source(ssb);
    //
    //         SearchResponse response = client.search(request, RequestOptions.DEFAULT);
    //         return parseOrderPage(response, page, size);
    //
    //     } catch (Exception e) {
    //         log.error("[ES降级] B端商户订单查询失败，触发降级。merchantId={}", merchantId, e);
    //         meterRegistry.counter("es_order_fallback_total",
    //                 "scene", "merchant_query").increment();
    //
    //         // 降级：Feign 调 trade-service（同样需要 TradeClient 开放接口）
    //         // List fallbackDocs = tradeClient.getOrdersByMerchantId(merchantId, page, size);
    //         // return PageDTO.of(fallbackDocs, (long) fallbackDocs.size(), page, size);
    //
    //         return PageDTO.empty(page, size);
    //     }
    // }

    /**
     * 工具：解析 order_index SearchResponse → PageDTO
     * @param response
     * @param page
     * @param size
     * @return
     */
    // 原问题：PageDTO.of(docs, total, page, size) 不存在
// 修正：手动构建 MybatisPlus Page，对齐 PageDTO 现有的 of() 签名
//
// 为什么用 Function 重载而不是 of(page, OrderDTO.class)？
//   → BeanUtils.copyList 只能做单层字段映射
//   → items 是 List，目标是 List
//   → 两层 nested 必须手动转，否则 items 全部为 null
    private PageDTO<OrderDTO> parseOrderPage(SearchResponse response, int page, int size) {
        SearchHits hits  = response.getHits();
        long       total = hits.getTotalHits().value;

        List<OrderDoc> docs = new ArrayList<>();
        for (SearchHit hit : hits.getHits()) {
            // Fastjson 处理 LocalDateTime：写入时若序列化为 "yyyy-MM-dd HH:mm:ss"
            // 需在 FastJsonConfig 里注册 LocalDateTimeDeserializer，此处直接 parseObject 即可
            OrderDoc doc = JSON.parseObject(hit.getSourceAsString(), OrderDoc.class);
            docs.add(doc);
        }

        // 手动构建 MybatisPlus Page，填入 ES 返回的分页信息
        Page<OrderDoc> mpPage = new Page<>(page, size);
        mpPage.setRecords(docs);
        mpPage.setTotal(total);
        mpPage.setPages((total + size - 1) / size);   // 向上取整，与 MP 默认行为一致

        // PageDTO.of(Page, Function)：逐条 Doc → DTO，手动处理 nested items
        return PageDTO.of(mpPage, doc -> {
            OrderDTO dto = BeanUtils.copyBean(doc, OrderDTO.class);

            // nested 两层：OrderDetailDoc → OrderDetailDTO
            if (doc.getItems() != null) {
                List<EsOrderDetailDTO> itemDTOs = doc.getItems().stream()
                        .map(detail -> BeanUtils.copyBean(detail, EsOrderDetailDTO.class))
                        .collect(java.util.stream.Collectors.toList());
                dto.setItems(itemDTOs);
            }

            return dto;
        });
    }


    // 模拟 PO 转 Doc 辅助方法
    private List<EsOrderDoc> convertToDoc(List<?> pos) {
        return new ArrayList<>(); // 具体属性拷贝略
    }

    @Override
    public List<ItemDTO> queryItemByIds(Collection<Long> ids) {
        return BeanUtils.copyList(listByIds(ids), ItemDTO.class);
    }

    @Override
    public PageDTO<ItemDTO> search(ItemPageQuery query) throws IOException {
        // 0.前端传递的分页参数
        int pageNo = query.getPageNo(), pageSize = query.getPageSize();
        // 1.创建Request对象
        SearchRequest request = new SearchRequest("items");
        // 2.组织DSL参数
        // 2.1.query条件
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        if (StrUtil.isNotBlank(query.getKey())){
            queryBuilder.must(QueryBuilders.matchQuery("name",query.getKey()));
        }
        if (StrUtil.isNotBlank(query.getBrand())) {
            queryBuilder.filter(QueryBuilders.termQuery("brand",query.getBrand()));
        }
        if (StrUtil.isNotBlank(query.getCategory())){
            queryBuilder.filter(QueryBuilders.termQuery("category",query.getCategory()));
        }
        if (query.getMaxPrice() != null){
            queryBuilder.filter(QueryBuilders.rangeQuery("price").lte(query.getMaxPrice()).gte(query.getMinPrice()));
        }
        // 算分查询
        FunctionScoreQueryBuilder isAD = QueryBuilders.functionScoreQuery(queryBuilder, new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                new FunctionScoreQueryBuilder.FilterFunctionBuilder
                        (QueryBuilders.termQuery("isAD", true),
                                ScoreFunctionBuilders.weightFactorFunction(100))
        }).boostMode(CombineFunction.MULTIPLY);
        // 对广告字段isAD=true的做一个特殊关照
        request.source().query(isAD);
        // 2.2.分页
        request.source().from((pageNo - 1) * pageSize).size(pageSize);

        // 2.3.排序
        List<OrderItem> orders = query.toMpPage("updateTime", false).orders();
        for (OrderItem orderItem : orders) {
            request.source().
                    sort(orderItem.getColumn(), orderItem.isAsc() ? SortOrder.ASC : SortOrder.DESC);
        }
//        request.source()
//                .sort("updateTime", SortOrder.DESC);
        // 3.发送请求
        SearchResponse response = null;
        try {
            response = client.search(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // 4.解析结果
        Page<ItemDoc> itemDocPage = parseResponseResult(response, query);
//        System.out.println("itemDocPage.getPages() = " + itemDocPage.getPages());

        return PageDTO.of(itemDocPage, ItemDTO.class);
    }
    private Page<ItemDoc> parseResponseResult(SearchResponse response, ItemPageQuery query) {
        SearchHits searchHits = response.getHits();
        // 4.1.总条数
        long total = searchHits.getTotalHits().value;
//        System.out.println("total = " + total);
        // 4.2.命中的数据
        SearchHit[] hits = searchHits.getHits();
        Page<ItemDoc> docPage = new Page<>();

        ArrayList<ItemDoc> itemDocs = new ArrayList<>();
        if (hits != null){
            for (SearchHit hit : hits) {
                // 4.2.1.获取source结果
                String json = hit.getSourceAsString();
                // 4.2.2.转为ItemDoc
                ItemDoc doc = JSONUtil.toBean(json, ItemDoc.class);
                // 4.3.处理高亮结果
                Map<String, HighlightField> hfs = hit.getHighlightFields();
                if (hfs != null && !hfs.isEmpty()){
                    // 4.3.1.根据高亮字段名获取高亮结果
                    HighlightField hf = hfs.get("name");
                    // 4.3.2.获取高亮结果，覆盖非高亮结果
                    String hfName = hf.getFragments()[0].string();//数组可能由于name长度超过阈值而产生多个切片，需要组合起来；此为简单案例
                    doc.setName(hfName);
                }
                itemDocs.add(doc);
            }
        }

        docPage.setRecords(itemDocs);
        docPage.setTotal(total);
        docPage.setSize(20);

//        System.out.println("docPage.getPageNo() = " + docPage.getPages());

        return docPage;
    }

////    @PostConstruct
//    void setUp() {//创建
//        client = new RestHighLevelClient(RestClient.builder(
//                HttpHost.create("http://192.168.31.128:9200")
//        ));
//    }
//
////    @PreDestroy
//    void tearDown() throws IOException {//销毁
//        if (client!=null){
//            client.close();
//        }
//    }

}
