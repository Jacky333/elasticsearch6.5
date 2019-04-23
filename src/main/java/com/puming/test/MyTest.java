package com.puming.test;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.date.TimeInterval;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author pengcheng
 * @version V1.0
 * @description 测试类
 * @date 2019/03/29 10:59
 */
public class MyTest {
    private static TransportClient getClient() throws UnknownHostException {
        // 指定es集群名称
        Settings set = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        // 创建访问es集群的客户端
        TransportClient client = new PreBuiltTransportClient(set)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.147.130"), 9300));
        return client;
    }

    // 从es中查询数据
    @Test
    public void testQuery() throws UnknownHostException {
        TimeInterval timer = DateUtil.timer();
        TransportClient client = getClient();
        GetResponse response = client.prepareGet("hardware", "equipment", "160009").execute().actionGet();
        System.out.println("花费时间：" + timer.interval());
        System.out.println(response.getSourceAsString());
        client.close();
    }

    //插入数据到es
    @Test
    public void testInsert() throws IOException {
        TransportClient client = getClient();
        XContentBuilder doc = XContentFactory.jsonBuilder().startObject()
                .field("id", "aa")
                .field("name", "摄像头")
                .field("createTime", new Date())
                .field("updateTime", new Date())
                .endObject();
        IndexResponse response = client.prepareIndex("hardware", "equipment", "160009").setSource(doc).get();
        System.out.println(response.status());
    }

    //es直接更新文档
    @Test
    public void testUpdate() throws IOException, ExecutionException, InterruptedException {
        TransportClient client = getClient();
        UpdateRequest request = new UpdateRequest();
        request.index("hardware").type("equipment").id("160009")
                .doc(XContentFactory.jsonBuilder().startObject().field("name", "campera")
                        .field("peopleIn", 20).field("peopleOut", 10).endObject());
        UpdateResponse response = client.update(request).get();
        System.out.println(response.status());
    }

    //如果文档不存在则插入存在则更新
    @Test
    public void testUpdate2() throws IOException, ExecutionException, InterruptedException {
        TransportClient client = getClient();
        IndexRequest request = new IndexRequest("hardware", "equipment", "160009")
                .source(XContentFactory.jsonBuilder().startObject()
                        .field("name", "campera").field("id", "aa")
                        .field("desc", "摄像头").endObject());
        UpdateRequest request2 = new UpdateRequest("hardware", "equipment", "160009")
                .doc(XContentFactory.jsonBuilder().startObject().field("id", "160009")
                        .field("name", "what is this").endObject()).upsert(request);
        UpdateResponse response = client.update(request2).get();
        System.out.println(response.status());
        client.close();
    }

    //删除
    @Test
    public void testDelete() throws UnknownHostException {
        TransportClient client = getClient();
        DeleteResponse response = client.prepareDelete("hardware", "equipment", "150009").get();
        System.out.println(response.status());
        client.close();
    }

    /**
     * 根据属性值查询删除
     */
    @Test
    public void deleteByQuery() throws UnknownHostException {
        TransportClient client = getClient();
        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery("id", "512949"))
                .source("hardware")
                .get();
        long deleted = response.getDeleted();
        System.out.println(deleted);
    }


    // multiGet批量查询
    @Test
    public void testMultiGet() throws UnknownHostException {
        TransportClient client = getClient();
        MultiGetResponse responses =
                client.prepareMultiGet().add("hardware", "equipment", "1008", "1009", "1010")
                        .add("hardware", "equipment", "1011", "1012", "1013")
                        .get();
        for (MultiGetItemResponse item : responses) {
            if (null != item.getResponse()) {
                System.out.println(item.getResponse().getSourceAsString());
            }
        }
    }

    // bulk批量操作
    @Test
    public void tetstBulk() throws IOException {
        TransportClient client = getClient();
        BulkRequestBuilder builder = client.prepareBulk();
        builder.add(client.prepareUpdate("hardware", "equipment", "160009")
                .setDoc(XContentFactory.jsonBuilder().startObject().field("id", "15001")
                        .field("name", "what the fuck is this").endObject()));
        builder.add(client.prepareIndex("hardware", "equipment", "160012")
                .setSource(XContentFactory.jsonBuilder().startObject().field("id", "15069")
                        .field("name", "jjjjjjj").endObject()));
        builder.add(client.prepareDelete("hardware", "equipment", "160010"));
        BulkResponse responses = builder.get();
        System.out.println(responses.status());
    }

    //匹配所有
    @Test
    public void testMatchAll() throws UnknownHostException {
        TransportClient client = getClient();
        MatchAllQueryBuilder allQuery = QueryBuilders.matchAllQuery();
        SearchResponse response = client.prepareSearch("hardware").
                setTypes("equipment").setQuery(allQuery)
                //默认查询10条
                .setFrom(0).setSize(10000)
                .get();
        for (SearchHit hit : response.getHits()) {
            System.out.println(hit.getSourceAsString());
            Map<String, Object> map = hit.getSourceAsMap();
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                System.out.println(entry.getKey() + ":" + entry.getValue().toString());
            }
        }
        //所有记录条数
        System.out.println(response.getHits().getTotalHits());
    }

    //条件查询
    @Test
    public void testMatch() throws UnknownHostException {
        TransportClient client = getClient();
        MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("name", "星期一");
        SearchResponse response = client.prepareSearch("hardware").setTypes("equipment").setQuery(matchQuery)
                .setSize(10).get();
        for (SearchHit hit : response.getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    //一个值多个字段去匹配
    @Test
    public void testMultiMatch() throws UnknownHostException {
        TransportClient client = getClient();
        //入参前面是值，右面是字段
        MultiMatchQueryBuilder matchQuery = QueryBuilders.multiMatchQuery("马云", "name", "id");
        SearchResponse response = client.prepareSearch("hardware").setTypes("equipment").setQuery(matchQuery)
                .setSize(20).get();
        for (SearchHit hit : response.getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    //准确查询term
    @Test
    public void testTerm() throws UnknownHostException {
        TransportClient client = getClient();
//        TermQueryBuilder termQuery = QueryBuilders.termQuery("peopleIn", 800);
        //查询名字没有显示 ，因为name类型是text，支持分词，查询的时候被分词为"鼠标"，"星期一","妹妹"，"水杯"
//        TermQueryBuilder termQuery = QueryBuilders.termQuery("name", "鼠标");
        MatchQueryBuilder termQuery = QueryBuilders.matchQuery("name", "鼠标");
        SearchResponse response = client.prepareSearch("hardware").setTypes("equipment").setQuery(termQuery)
                .setSize(12).get();
        for (SearchHit hit : response.getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    //一个字段多个值terms查询
    @Test
    public void testTerms() throws UnknownHostException {
        TransportClient client = getClient();
        TermsQueryBuilder termsQuery = QueryBuilders.termsQuery("id", "248", "300");
        SearchResponse response = client.prepareSearch("hardware").setTypes("equipment").setQuery(termsQuery)
                .setSize(30).get();
        for (SearchHit hit : response.getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    //各种查询
    @Test
    public void testManyQuery() throws UnknownHostException, ParseException {
        TransportClient client = getClient();
        //TODO 日期查询有问题
        // 范围查询range
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        Date createTime = sdf.parse("2017-1-1 00:00:00");
//        Date createTime2 = sdf.parse("2017-1-9 00:00:00");
//        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("createTime").from(createTime).to(createTime2);
//        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("peopleIn").from(100).to(150);
//        SearchResponse response = client.prepareSearch("hardware").setTypes("equipment").setQuery(rangeQuery).get();
        //前缀匹配
//        PrefixQueryBuilder query = QueryBuilders.prefixQuery("id", "5");

        //模糊（通配符）查询
//        WildcardQueryBuilder query = QueryBuilders.wildcardQuery("id", "*5");
        //模糊查询（暂时不知道其作用,只会查出一个）
//        FuzzyQueryBuilder query = QueryBuilders.fuzzyQuery("id", "6");
        // 类型查询type(暂时不知道其作用)
//        TypeQueryBuilder query = QueryBuilders.typeQuery("id");
        // ids查询
        IdsQueryBuilder query = QueryBuilders.idsQuery("id").addIds("2120", "3600");
        SearchResponse response = client.prepareSearch("hardware").setTypes("equipment").setQuery(query).setSize(18).get();
        for (SearchHit hit : response.getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    // 聚合查询
    @Test
    public void testAggregation() throws UnknownHostException {
        TransportClient client = getClient();
        //查询最大值
//        MaxAggregationBuilder builder = AggregationBuilders.max("max").field("peopleIn");
//        SearchResponse response = client.prepareSearch("hardware").setTypes("equipment").addAggregation(builder).get();
//        Max valueCount = response.getAggregations().get("max");
//        double value = valueCount.getValue();

        //查询最小值
//        MinAggregationBuilder builder = AggregationBuilders.min("min").field("peopleIn");
//        SearchResponse response = client.prepareSearch("hardware").setTypes("equipment").addAggregation(builder).get();
//        Min valueCount = response.getAggregations().get("min");
//        double value = valueCount.getValue();

        //查询总和
//        SumAggregationBuilder builder = AggregationBuilders.sum("sum").field("peopleIn");
//        SearchResponse response = client.prepareSearch("hardware").setTypes("equipment").addAggregation(builder).get();
//        Sum valueCount = response.getAggregations().get("sum");
//        double value = valueCount.getValue();

        //查询平均值
//        AvgAggregationBuilder builder = AggregationBuilders.avg("avg").field("peopleIn");
//        SearchResponse response = client.prepareSearch("hardware").setTypes("equipment").addAggregation(builder).get();
//        Avg valueCount = response.getAggregations().get("avg");
//        double value = valueCount.getValue();

        //基数
//        CardinalityAggregationBuilder builder = AggregationBuilders.cardinality("aaa").field("peopleIn");
//        SearchResponse response = client.prepareSearch("hardware").setTypes("hardware").addAggregation(builder).get();
//        Cardinality valueCount = response.getAggregations().get("aaa");
//        double value = valueCount.getValue();

        //计数
        AggregationBuilder builder = AggregationBuilders.count("nameCount").field("peopleIn");
        SearchResponse response = client.prepareSearch("hardware").setTypes("equipment").addAggregation(builder).get();
        ValueCount valueCount = response.getAggregations().get("nameCount");
        long value = valueCount.getValue();
        System.out.println(value);
    }

    //分组求数据
    @Test
    public void testGroup() throws UnknownHostException {
        TransportClient client = getClient();
        TermsAggregationBuilder termsBuilder = AggregationBuilders.terms("by_people_in").field("peopleIn");
        AggregationBuilder sumBuilder = AggregationBuilders.sum("peopleInSum").field("peopleIn");
        AggregationBuilder avgBuilder = AggregationBuilders.avg("peopleInAvg").field("peopleIn");
        AggregationBuilder countBuilder = AggregationBuilders.count("peopleInCount").field("peopleIn");
        termsBuilder.subAggregation(sumBuilder).subAggregation(avgBuilder).subAggregation(countBuilder);
        SearchResponse response = client.prepareSearch("hardware").setTypes("equipment").addAggregation(termsBuilder).get();
        Aggregations terms = response.getAggregations();
        for (Aggregation a : terms) {
            LongTerms teamSum = (LongTerms) a;
            for (LongTerms.Bucket bucket : teamSum.getBuckets()) {
                System.out.println(bucket.getKeyAsString() + "   " + bucket.getDocCount() + "    " + ((Sum) bucket.getAggregations().asMap()
                        .get("peopleInSum")).getValue() + "    " + ((Avg) bucket.getAggregations().asMap().get("peopleInAvg")).getValue() + "    "
                        + ((ValueCount) bucket.getAggregations().asMap().get("peopleInCount")).getValue());
            }
        }
    }

    //统计样本基本指标
    @Test
    public void testBasic() throws UnknownHostException {
        TransportClient client = getClient();
        AggregationBuilder termsBuilder = AggregationBuilders.stats("stats").field("peopleIn");
        SearchResponse response = client.prepareSearch("hardware").setTypes("equipment").addAggregation(termsBuilder).get();
        //import org.elasticsearch.search.aggregations.metrics.stats.Stats;
        Stats valueCount = response.getAggregations().get("stats");
        System.out.println("max :" + valueCount.getMaxAsString());
        System.out.println("min " + valueCount.getMinAsString());
        System.out.println("avg " + valueCount.getAvgAsString());
        System.out.println("sum " + valueCount.getSumAsString());
        System.out.println("count :" + valueCount.getCount());
    }

    //多分组求各组数据
    @Test
    public void testGroup2() throws UnknownHostException {
        TransportClient client = getClient();
        TermsAggregationBuilder all = AggregationBuilders.terms("by_people_in").field("peopleIn");
        TermsAggregationBuilder peopleOut = AggregationBuilders.terms("by_people_out").field("peopleOut");
        AggregationBuilder sumBuilder = AggregationBuilders.sum("peopleSum").field("peopleOut");
        all.subAggregation(peopleOut.subAggregation(sumBuilder));
        SearchResponse response = client.prepareSearch("hardware").setTypes("equipment").addAggregation(all).get();
        Aggregations terms = response.getAggregations();
        for (Aggregation a : terms) {
            LongTerms teamSum = (LongTerms) a;
            for (LongTerms.Bucket bucket : teamSum.getBuckets()) {
                Aggregation peopleOuts = bucket.getAggregations().getAsMap().get("by_people_out");
                LongTerms terms1 = (LongTerms) peopleOuts;
                for (LongTerms.Bucket bu : terms1.getBuckets()) {
                    System.out.println(bucket.getKeyAsString() + "  " + bu.getKeyAsString() + " "
                            + bu.getDocCount() + "    " + ((Sum) bu.getAggregations().asMap().get("peopleSum")).getValue());
                }
            }
        }

    }

    //组合查询
    @Test
    public void testMuchQuery() throws UnknownHostException {
        TransportClient client = getClient();
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("peopleIn", 500))
                .mustNot(QueryBuilders.matchQuery("peopleOut", 500))
                .should(QueryBuilders.matchQuery("name", "马云"))
                .filter(QueryBuilders.rangeQuery("createTime").from("2017-01-01").to("2017-01-03").format("yyyy-MM-dd"));

        SearchResponse response = client.prepareSearch("hardware").setTypes("equipment").setQuery(queryBuilder).get();
        for (SearchHit hit : response.getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    //query string
    @Test
    public void testQueryString() throws UnknownHostException {
        TransportClient client = getClient();
        CommonTermsQueryBuilder builder = QueryBuilders.commonTermsQuery("name", "马云");
//        QueryStringQueryBuilder builder = QueryBuilders.queryStringQuery("马云");
//        SimpleQueryStringBuilder builder = QueryBuilders.simpleQueryStringQuery("马云");
        SearchResponse response = client.prepareSearch("hardware").setTypes("equipment").setQuery(builder).get();
        for (SearchHit hit : response.getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    //创建索引结构和索引
    @Test
    public void testCreateIndexSturc() throws IOException {
        TransportClient client = getClient();
        XContentBuilder builder = XContentFactory
                .jsonBuilder()
                .startObject()
                    .startObject("data")
                        .startObject("properties")
                            .startObject("id")
                                .field("type", "text")
                                .field("store", true)
                                .field("index", true)
                        .endObject()
                        .startObject("name")
                                .field("type", "text")
                                .field("store", true)
                                .field("index", true)
                        .endObject()
                        .startObject("role_id")
                                .field("type", "integer")
                                .field("store", true)
                                .field("index", true)
                        .endObject()
                        .startObject("c_time")
                                .field("type", "date")
                                .field("store", true)
                                .field("index", true)
                                .field("format","strict_date_optional_time||epoch_millis||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd")
                        .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        client.admin().indices().prepareCreate("user").execute().actionGet();
        PutMappingRequest mapping = Requests.putMappingRequest("user").type("data").source(builder);
        client.admin().indices().putMapping(mapping).actionGet();
    }

    //删除索引
    @Test
    public void deleteIndex() throws UnknownHostException {
        TransportClient client = getClient();
        AcknowledgedResponse response = client.admin().indices().prepareDelete("tt").execute().actionGet();
        if(response.isAcknowledged()){
            System.out.println("删除成功");
        }else {
            System.out.println("删除失败");
        }

    }

    //分页查询，from,size
    @Test
    public void queryByPage() throws UnknownHostException {
        TimeInterval timer = DateUtil.timer();
        TransportClient client = getClient();
        //field字段中不能用text类型，Fielddata默认情况下禁用文本字段，因为Fielddata可以消耗大量的堆空间
        AggregationBuilder termsBuilder = AggregationBuilders.stats("stats").field("peopleOut");
        SearchResponse response = client.prepareSearch("hardware").setTypes("equipment").addAggregation(termsBuilder).get();
        Stats valueCount = response.getAggregations().get("stats");
        long count = valueCount.getCount();
        SearchRequestBuilder requestBuilder = client.prepareSearch("hardware").setTypes("equipment").setQuery(
                QueryBuilders.matchQuery("name", "马云")
        );
        for (int i = 0, sum = 0; sum < count; i++) {
            SearchResponse searchResponse = requestBuilder.setFrom(i).setSize(1000).execute().actionGet();
            sum+=searchResponse.getHits().getHits().length;
            System.out.println("总量"+count+" 已经查到"+sum);
        }
        System.out.println("花费时间：" + timer.interval());
    }

    @Test
    public void queryByPageScroll() throws UnknownHostException {
        TimeInterval timer = DateUtil.timer();
        TransportClient client = getClient();
        SearchResponse scrollResponse  = client.prepareSearch("hardware").setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setSize(1000).setScroll(TimeValue.timeValueMinutes(1)).execute().actionGet();
        long count = scrollResponse.getHits().getTotalHits();
        for(int i=0,sum=0; sum<=count; i++){
            scrollResponse = client.prepareSearchScroll(scrollResponse.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(8))
                    .get();
            sum += scrollResponse.getHits().getHits().length;
            System.out.println("总量"+count+" 已经查到"+sum);
        }
        System.out.println("花费时间：" + timer.interval());
    }
}
