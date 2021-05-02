package com.me.gmallpublisher.service.impl;

import com.alibaba.fastjson.JSON;
import com.me.constants.GmallConstants;
import com.me.gmallpublisher.bean.Options;
import com.me.gmallpublisher.bean.Stat;
import com.me.gmallpublisher.mapper.OrderMapper;
import com.me.gmallpublisher.mapper.DauMapper;
import com.me.gmallpublisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;

    @Autowired
    private OrderMapper orderMapper;

    @Autowired
    private JestClient jestClient;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map<String, Long> getDauHour(String date) {
        //1.获取数据 => 获取mapper传过来的数据
        List<Map> mapList = dauMapper.selectDauTotalHourMap(date);

        //2.创建Map集合存放结果数据
        HashMap<String, Long> result = new HashMap<>();

        //3.解析mapper传过来的数据，并封装到新的Map集合中
        for (Map map : mapList) {
            result.put(map.get("LH").toString(), (Long) map.get("CT"));
        }


        return result;
    }

    @Override
    public Double getGmvTotal(String date) {
        //1.从mapper层获取数据
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map<String, Double> getGmvHour(String date) {
        //1.获取数据 => 获取mapper传过来的数据
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);

        //2.创建Map集合存放结果数据
        HashMap<String, Double> result = new HashMap<>();

        //3.解析mapper传过来的数据，并封装到新的Map集合中
        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"), (Double) map.get("SUM_AMOUNT"));
        }

        return result;
    }

    @Override
    public String getSaleDetail(String date, Integer start, Integer size, String keyword) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤 匹配
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt", date));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name", keyword).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);

        //  性别聚合
        TermsBuilder genderAggs = AggregationBuilders.terms("groupby_user_gender").field("user_gender").size(2);
        searchSourceBuilder.aggregation(genderAggs);

        //  年龄聚合
        TermsBuilder ageAggs = AggregationBuilders.terms("groupby_user_age").field("user_age").size(100);
        searchSourceBuilder.aggregation(ageAggs);

        // 行号= （页面-1） * 每页行数
        searchSourceBuilder.from((start - 1) * size);
        searchSourceBuilder.size(size);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstants.ES_QUERY_INDEXNAME).addType("_doc").build();

        SearchResult searchResult = jestClient.execute(search);

        //TODO 命中数据条数
        Long total = searchResult.getTotal();

        //TODO 获取数据详情
        ArrayList<Map> detail = new ArrayList<>();
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            detail.add(hit.source);
        }
        //TODO 获取年龄占比聚合组数据
        MetricAggregation ageAggregations = searchResult.getAggregations();
        TermsAggregation groupbyUserAge = ageAggregations.getTermsAggregation("groupby_user_age");
        List<TermsAggregation.Entry> ageBuckets = groupbyUserAge.getBuckets();
        Long low20Count = 0L;
        Long up30Count = 0L;
        for (TermsAggregation.Entry ageBucket : ageBuckets) {
            if (Integer.parseInt(ageBucket.getKey())<20){
                low20Count+=ageBucket.getCount();
            }else if (Integer.parseInt(ageBucket.getKey())>=30){
                up30Count += ageBucket.getCount();
            }
        }
        //获取小于20岁的年龄占比
        Double low20Ratio = Math.round(low20Count * 100D / total * 10D) / 10D;

        //获取大于30岁的年龄占比
        Double up30Ratio = Math.round(up30Count * 100D / total * 10D) / 10D;

        //20岁到30岁年龄占比
//        Double up20AndLow30Ratio = Math.round((100D - low20Ratio - up30Ratio) * 10D) / 10D;
        Double up20AndLow30Ratio = Math.round((total-low20Count-up30Count)*100D/total * 10D) / 10D;
        Options low20Opt = new Options("20岁以下", low20Ratio);
        Options up20AndLow30Opt = new Options("20岁到30岁", up20AndLow30Ratio);
        Options up30Opt = new Options("30岁及30岁以上", up30Ratio);

        //创建List集合用来存放年龄聚合组的对象
        ArrayList<Options> ageOptList = new ArrayList<>();
        ageOptList.add(low20Opt);
        ageOptList.add(up20AndLow30Opt);
        ageOptList.add(up30Opt);


        //TODO 获取性别占比聚合组数据
        TermsAggregation userGender = ageAggregations.getTermsAggregation("groupby_user_gender");
        List<TermsAggregation.Entry> genderbuckets = userGender.getBuckets();
        Long maleCount = 0L;
        for (TermsAggregation.Entry genderbucket : genderbuckets) {
            if ("M".equals(genderbucket.getKey())){
                maleCount += genderbucket.getCount();
            }
        }
        //获取男生性别占比
        Double maleRatio = Math.round(maleCount * 100D / total * 10D) / 10D;
        //获取女生性别占比
        Double femaleRatio = Math.round((100D-maleRatio)*10D) / 10D;

        Options maleOpt = new Options("男", maleRatio);
        Options femaleOpt = new Options("女", femaleRatio);

        //创建存放性别的list集合
        ArrayList<Options> genderList = new ArrayList<>();
        genderList.add(maleOpt);
        genderList.add(femaleOpt);

        //创建年龄的Stat对象
        Stat ageStat = new Stat(ageOptList, "用户年龄占比");

        //创建性别的Stat的对象
        Stat genderStat = new Stat(genderList, "用户性别占比");

        //创建存放Stat对象的list集合
        ArrayList<Stat> stats = new ArrayList<>();
        stats.add(ageStat);
        stats.add(genderStat);

        //创建Map集合用来存放结果数据
        HashMap<String, Object> result = new HashMap<>();
        result.put("total", total);
        result.put("stat", stats);
        result.put("detail", detail);

        return JSON.toJSONString(result);
    }
}
