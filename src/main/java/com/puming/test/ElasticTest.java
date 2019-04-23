package com.puming.test;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

public class ElasticTest {

    public static TransportClient getClient() throws UnknownHostException {
        // 指定es集群名称
        Settings set = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        // 创建访问es集群的客户端
        TransportClient client = new PreBuiltTransportClient(set)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.147.130"), 9300));
        return client;
    }

    // 从es中查询数据
    @Test
    public void test1() throws UnknownHostException {
        // 指定es集群名称
        Settings set = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        // 创建访问es集群的客户端
        TransportClient client = new PreBuiltTransportClient(set)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.147.130"), 9300));
        // 数据查询
        GetResponse response = client.prepareGet("bank", "account", "1").execute().actionGet();
        System.out.println(response.getSourceAsString());
        client.close();
    }

    // 给es添加文档
    @Test
    public void test2() throws IOException {
        TransportClient client = getClient();
        XContentBuilder doc = XContentFactory.jsonBuilder().startObject().field("id", "1")
                .field("title", "Java设计模式之装饰模式").field("content", "在不必改变原类文件和使用继承的情况下，动态的扩展一个类的功能")
                .field("postdate", "2018-05-20").field("url", "com.puming/elastic6.5").endObject();
        IndexResponse response = client.prepareIndex("index1", "blog", "10").setSource(doc).get();
        System.out.println(response.status());
    }

    // 删除文档
    @Test
    public void test3() throws IOException {
        TransportClient client = getClient();
        DeleteResponse response = client.prepareDelete("index1", "blog", "10").get();
        System.out.println(response.status());
    }

    // 跟新文档
    @Test
    public void test4() throws IOException, InterruptedException, ExecutionException {
        TransportClient client = getClient();
        UpdateRequest request = new UpdateRequest();
        request.index("index1").type("blog").id("10")
                .doc(XContentFactory.jsonBuilder().startObject().field("title", "单例莫设计模式").endObject());
        UpdateResponse response = client.update(request).get();
        System.out.println(response.status());
    }

    // 跟新文档upsert
    @Test
    public void test5() throws IOException, InterruptedException, ExecutionException {
        TransportClient client = getClient();

        IndexRequest request = new IndexRequest("index1", "blog", "8")
                .source(XContentFactory.jsonBuilder().startObject().field("id", "1").field("title", "Java设计模式之装饰模式")
                        .field("content", "在不必改变原类文件和使用继承的情况下，动态的扩展一个类的功能").field("postdate", "2018-05-20")
                        .field("url", "com.puming/elastic6.5").endObject());

        UpdateRequest request2 = new UpdateRequest("index1", "blog", "8").doc(
                XContentFactory.jsonBuilder().startObject().field("title", "工厂代理设计模式").field("id", "2").endObject())
                .upsert(request);
        UpdateResponse response = client.update(request2).get();
        System.out.println(response.status());
    }

    // multiGet批量查询
    @Test
    public void test6() throws IOException, InterruptedException, ExecutionException {
        TransportClient client = getClient();
        MultiGetResponse response = client.prepareMultiGet().add("index1", "blog", "8", "10")
                .add("lib3", "user", "1", "2", "3").get();

        for (MultiGetItemResponse item : response) {
            if (null != item.getResponse()) {
                System.out.println(item.getResponse().getSourceAsString());
            }
        }
    }

    // bulk批量操作
    @Test
    public void test7() throws IOException, InterruptedException, ExecutionException {
        TransportClient client = getClient();
        BulkRequestBuilder builder = client.prepareBulk();
        builder.add(client.prepareIndex("lib2", "books", "8").setSource(
                XContentFactory.jsonBuilder().startObject().field("title", "python").field("price", "98").endObject()));
        builder.add(client.prepareIndex("lib2", "books", "10")
                .setSource(XContentFactory.jsonBuilder().startObject().field("title", "c--").endObject()));
        BulkResponse response = builder.get();
        System.out.println(response.status());
        if (response.hasFailures()) {
            System.out.println("添加失败了");
        }
    }

    // 查询删除
    @Test
    public void test8() throws IOException, InterruptedException, ExecutionException {
        TransportClient client = getClient();
        BulkByScrollResponse scrollResponse = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery("title", "工厂")).source("index1").get();
        long counts = scrollResponse.getDeleted();
        System.out.println(counts);
    }

    // match_all
    @Test
    public void test9() throws IOException, InterruptedException, ExecutionException {
        TransportClient client = getClient();
        MatchAllQueryBuilder allQuery = QueryBuilders.matchAllQuery();
        SearchResponse response = client.prepareSearch("lib3").setQuery(allQuery).get();
        for (SearchHit hit : response.getHits()) {
            System.out.println(hit.getSourceAsString());
            Map<String, Object> map = hit.getSourceAsMap();
            for (Entry<String, Object> entry : map.entrySet()) {
                System.out.println(entry.getKey() + ":" + entry.getValue().toString());
            }
        }
    }

    // match_query
    @Test
    public void test10() throws IOException, InterruptedException, ExecutionException {
        TransportClient client = getClient();
        MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("interests", "唱歌");
        SearchResponse response = client.prepareSearch("lib3").setQuery(matchQuery).setSize(10).get();
        for (SearchHit hit : response.getHits()) {
            System.out.println(hit.getSourceAsString());
            Map<String, Object> map = hit.getSourceAsMap();
            for (Entry<String, Object> entry : map.entrySet()) {
                System.out.println(entry.getKey() + ":" + entry.getValue().toString());
            }
        }
    }

    // Multi_match_query
    @Test
    public void test11() throws IOException, InterruptedException, ExecutionException {
        TransportClient client = getClient();
        MultiMatchQueryBuilder multiMatchQuery = QueryBuilders.multiMatchQuery("changge", "address", "interests");
        SearchResponse response = client.prepareSearch("lib3").setQuery(multiMatchQuery).setSize(10).get();
        for (SearchHit hit : response.getHits()) {
            System.out.println(hit.getSourceAsString());
            Map<String, Object> map = hit.getSourceAsMap();
            for (Entry<String, Object> entry : map.entrySet()) {
                System.out.println(entry.getKey() + ":" + entry.getValue().toString());
            }
        }
    }

    // term查询
    @Test
    public void test12() throws IOException, InterruptedException, ExecutionException {
        TransportClient client = getClient();
        TermQueryBuilder termQuery = QueryBuilders.termQuery("interests", "changge");
        SearchResponse response = client.prepareSearch("lib3").setQuery(termQuery).setSize(10).get();
        for (SearchHit hit : response.getHits()) {
            System.out.println(hit.getSourceAsString());
            Map<String, Object> map = hit.getSourceAsMap();
            for (Entry<String, Object> entry : map.entrySet()) {
                System.out.println(entry.getKey() + ":" + entry.getValue().toString());
            }
        }
    }

    // terms查询
    @Test
    public void test13() throws IOException, InterruptedException, ExecutionException {
        TransportClient client = getClient();
        TermsQueryBuilder termsQuery = QueryBuilders.termsQuery("interests", "changge", "唱歌");
        SearchResponse response = client.prepareSearch("lib3").setQuery(termsQuery).setSize(10).get();
        for (SearchHit hit : response.getHits()) {
            System.out.println(hit.getSourceAsString());
            Map<String, Object> map = hit.getSourceAsMap();
            for (Entry<String, Object> entry : map.entrySet()) {
                System.out.println(entry.getKey() + ":" + entry.getValue().toString());
            }
        }
    }

    // 各种查询max
    @Test
    public void test14() throws IOException, InterruptedException, ExecutionException {
        TransportClient client = getClient();
        // 范围查询range
        RangeQueryBuilder builder = QueryBuilders.rangeQuery("birthday").from("1990-01-01").to("1999-12-31")
                .format("yy-MM-dd");
        // prefix
        PrefixQueryBuilder prefixQuery = QueryBuilders.prefixQuery("name", "张三");
        // 模糊查询wildcard
        WildcardQueryBuilder wildcardQuery = QueryBuilders.wildcardQuery("name", "张*");
        // 模糊查询fuzzy
        FuzzyQueryBuilder fuzzyQuery = QueryBuilders.fuzzyQuery("interests", "change");
        // 类型查询type
        TypeQueryBuilder typeQuery = QueryBuilders.typeQuery("user");
        // ids查询
        IdsQueryBuilder idsQuery = QueryBuilders.idsQuery("user").addIds("1", "2");
        SearchResponse response = client.prepareSearch("lib3").setQuery(idsQuery).get();
        for (SearchHit hit : response.getHits()) {
            System.out.println(hit.getSourceAsString());
            Map<String, Object> map = hit.getSourceAsMap();
            for (Entry<String, Object> entry : map.entrySet()) {
                System.out.println(entry.getKey() + ":" + entry.getValue().toString());
            }
        }
    }

    // 聚合查询
    @Test
    public void test15() throws IOException, InterruptedException, ExecutionException {
        TransportClient client = getClient();
        // max
        MaxAggregationBuilder maxBuilder = AggregationBuilders.max("aggMax").field("age");
        // min
        AggregationBuilder minBuilder = AggregationBuilders.min("aaa").field("age");
        // avg
        AggregationBuilder avgBuilder = AggregationBuilders.avg("aaa").field("age");
        // sum
        AggregationBuilder sumBuilder = AggregationBuilders.sum("aaa").field("age");
        // cardinality
        AggregationBuilder cardinalityBuilder = AggregationBuilders.cardinality("aaa").field("age");
        SearchResponse response = client.prepareSearch("lib3").addAggregation(cardinalityBuilder).get();
        // Max max = response.getAggregations().get("aggMax");
        // Min min = response.getAggregations().get("aaa");
        // Avg min = response.getAggregations().get("aaa");
        // Sum min = response.getAggregations().get("aaa");
        Cardinality min = response.getAggregations().get("aaa");
        System.out.println(min.getValue());
    }

    // query string
    @Test
    public void test20() throws IOException, InterruptedException, ExecutionException {
        TransportClient client = getClient();
        CommonTermsQueryBuilder commonTermsQuery = QueryBuilders.commonTermsQuery("address", "海淀区");
        QueryStringQueryBuilder queryStringQuery = QueryBuilders.queryStringQuery("-唱歌 +喝酒");
        SimpleQueryStringBuilder simpleQueryStringQuery = QueryBuilders.simpleQueryStringQuery("-唱歌 +喝酒");
        SearchResponse response = client.prepareSearch("lib3").setQuery(commonTermsQuery).get();
        for (SearchHit hit : response.getHits()) {
            System.out.println(hit.getSourceAsString());
            Map<String, Object> map = hit.getSourceAsMap();
            for (Entry<String, Object> entry : map.entrySet()) {
                System.out.println(entry.getKey() + ":" + entry.getValue().toString());
            }
        }
    }

    // 组合查询
    @Test
    public void test21() throws IOException, InterruptedException, ExecutionException {
        TransportClient client = getClient();
        BoolQueryBuilder builder = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("interests", "喝酒"))
                .mustNot(QueryBuilders.matchQuery("interests", "唱歌"))
                .should(QueryBuilders.matchQuery("address", "海淀区"))
                .filter(QueryBuilders.rangeQuery("birthday").from("1970-01-01").to("2011-12-12").format("yy-MM-dd"));

        SearchResponse response = client.prepareSearch("lib3").setQuery(builder).get();
        for (SearchHit hit : response.getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

}
