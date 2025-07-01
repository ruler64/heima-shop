package com.hmall.search.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.metadata.OrderItem;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmall.search.domain.po.Item;
import com.hmall.search.domain.po.ItemDoc;
import com.hmall.search.domain.query.ItemPageQuery;
import com.hmall.search.mapper.SearchMapper;
import com.hmall.search.service.ISearchService;
import com.hmall.api.dto.ItemDTO;
import com.hmall.common.domain.PageDTO;
import com.hmall.common.utils.BeanUtils;

import lombok.RequiredArgsConstructor;
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
public class SearchServiceImpl extends ServiceImpl<SearchMapper, Item> implements ISearchService {

    private final RestHighLevelClient client= new RestHighLevelClient(RestClient.builder(
            HttpHost.create("http://192.168.31.128:9200")));

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
