package com.puming.equipment;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.date.TimeInterval;
import com.google.gson.Gson;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.joda.time.DateTime;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author pengcheng
 * @version V1.0
 * @description 查询设备信息
 * @date 2019/03/30 14:20
 */
public class QueryEquipment {
    private static TransportClient getClient() throws UnknownHostException {
        // 指定es集群名称
        Settings set = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        // 创建访问es集群的客户端
        TransportClient client = new PreBuiltTransportClient(set)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.147.130"), 9300));
        return client;
    }

    /**
     * 根据设备名称查询
     *
     * @throws UnknownHostException
     */
    @Test
    public void testMatch() throws UnknownHostException {
        TransportClient client = getClient();
        MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("name", "马云");
        SearchResponse response = client.prepareSearch("hardware").setTypes("equipment").setQuery(matchQuery)
                .setSize(25).get();
        for (SearchHit hit : response.getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * 查询最近一年,出(入)的人流量总和\平均数
     */
    @Test
    public void testGroup() throws UnknownHostException, ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date time1 = sdf.parse("2017-01-01 00:00:00");
        Date time2 = sdf.parse("2017-12-31 23:59:59");
        TransportClient client = getClient();
        RangeQueryBuilder queryBuilder = QueryBuilders.rangeQuery("createTime").from(time1.getTime()).to(time2.getTime());
        AggregationBuilder termsBuilder = AggregationBuilders.stats("stats").field("peopleIn");
        SearchResponse response = client.prepareSearch("hardware")
                .setTypes("equipment").setQuery(queryBuilder).addAggregation(termsBuilder).get();
        Stats valueCount = response.getAggregations().get("stats");
        System.out.println("人数总和： " + valueCount.getSumAsString());
        System.out.println("人数平均数： " + valueCount.getAvgAsString());
        System.out.println("人数最多： " + valueCount.getMaxAsString());
        System.out.println("人数最少： " + valueCount.getMinAsString());
    }

    //最近一年内,每月,每日,出(入)的人流量总和\平均数
    @Test
    public void testGroup2() throws ParseException, UnknownHostException {
        long startTime = System.currentTimeMillis();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date time1 = sdf.parse("2017-01-01 00:00:00");
        Date time2 = sdf.parse("2017-12-31 23:59:59");
        TransportClient client = getClient();
        RangeQueryBuilder queryBuilder = QueryBuilders.rangeQuery("createTime").from(time1.getTime()).to(time2.getTime());
        TermsAggregationBuilder termsBuilder = AggregationBuilders.terms("by_createTime").field("createTime").format("yyyy-MM-dd");
        SumAggregationBuilder sumInBuilder = AggregationBuilders.sum("peopleInSum").field("peopleIn");
        AvgAggregationBuilder avgInBuilder = AggregationBuilders.avg("peopleInAvg").field("peopleIn");
        SumAggregationBuilder sumOutBuilder = AggregationBuilders.sum("peopleOutSum").field("peopleOut");
        AvgAggregationBuilder avgOutBuilder = AggregationBuilders.avg("peopleOutAvg").field("peopleOut");
        termsBuilder.subAggregation(sumInBuilder).subAggregation(avgInBuilder)
                .subAggregation(sumOutBuilder).subAggregation(avgOutBuilder);

        SearchResponse response = client.prepareSearch("hardware").setTypes("equipment")
                .setQuery(queryBuilder).addAggregation(termsBuilder).setSize(100).get();
        Aggregations terms = response.getAggregations();
        for (Aggregation a : terms) {
            LongTerms teamSum = (LongTerms) a;
            for (LongTerms.Bucket bucket : teamSum.getBuckets()) {
                System.out.println(bucket.getKeyAsString() + "   " + bucket.getDocCount() + "    " + ((Sum) bucket.getAggregations().asMap()
                        .get("peopleInSum")).getValue() + "    " + ((Avg) bucket.getAggregations().asMap().get("peopleInAvg")).getValue()
                        + "    " + ((Sum) bucket.getAggregations().asMap().get("peopleOutSum")).getValue()
                        + "    " + ((Avg) bucket.getAggregations().asMap().get("peopleOutAvg")).getValue()
                );
            }
        }
        long endTime = System.currentTimeMillis();
        System.out.println("运行时间:"+(endTime-startTime)+"ms");
    }

    //每年,月，日数据数
    @Test
    public void testGroup3() throws UnknownHostException {
        TransportClient client = getClient();
        AggregationBuilder aggregation
                = AggregationBuilders.dateHistogram("agg").field("createTime").dateHistogramInterval(DateHistogramInterval.MONTH);
        //每隔10天
//                = AggregationBuilders.dateHistogram("agg").field("createTime").dateHistogramInterval(DateHistogramInterval.days(10));
        SearchResponse response =
                client.prepareSearch("hardware").setTypes("equipment").addAggregation(aggregation).execute().actionGet();
        Histogram agg = response.getAggregations().get("agg");

        for (Histogram.Bucket entry : agg.getBuckets()) {
            DateTime key = (DateTime) entry.getKey();
            String keyAsString = entry.getKeyAsString();
            long docCount = entry.getDocCount();
            System.out.println("key:" + key + ",keyAsString:" + keyAsString + ",docCount:" + docCount);
        }
    }

    @Test
    public void testGroup4() throws Exception {
        TimeInterval timer = DateUtil.timer();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date time1 = sdf.parse("2017-01-01 08:00:00");
        Date time2 = sdf.parse("2017-12-31 23:59:59");
        TransportClient client = getClient();
        AggregationBuilder dateHistogram
                = AggregationBuilders.dateHistogram("agg").field("createTime").dateHistogramInterval(DateHistogramInterval.MONTH);
        StatsAggregationBuilder stats = AggregationBuilders.stats("stats").field("peopleIn");
        RangeQueryBuilder builder = QueryBuilders.rangeQuery("createTime").from(time1.getTime()).to(time2.getTime());
        dateHistogram.subAggregation(stats);
        SearchResponse response =
                client.prepareSearch("hardware").setTypes("equipment").setQuery(builder).addAggregation(dateHistogram).get();
        Histogram timeAgg = response.getAggregations().get("agg");
        for (Histogram.Bucket entry : timeAgg.getBuckets()) {
            System.out.println(entry.getKey() + "-----" + entry.getDocCount());
            Aggregation aggregation = entry.getAggregations().get("stats");
            Entity entity = new Gson().fromJson(aggregation.toString(), Entity.class);
            System.out.println("max         "+entity.getStats().getMax());
            System.out.println("min         "+entity.getStats().getMin());
            System.out.println("avg         "+entity.getStats().getAvg());
            System.out.println("sum         "+entity.getStats().getSum());
            System.out.println("count       "+entity.getStats().getCount());
        }

        System.out.println("花费时间："+timer.interval());
    }
}
