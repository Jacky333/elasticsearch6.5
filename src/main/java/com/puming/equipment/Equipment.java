package com.puming.equipment;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Random;

public class Equipment {

    private static final String[] NAMES = new String[]{"名称", "puming", "设备", "全能", "坚硬", "石膏板", "篮球", "娱乐", "人生",
            "大师", "唐嫣", "妹妹", "水杯", "公司", "星期一", "同学", "高性能", "神仙", "机械", "小妖", "后宫", "物品", "打磨", "石化", "造成", "氧气",
            "水银", "铂金", "强度", "成功", "马云", "伯乐", "巴黎", "焊接", "装配", "包装", "金属", "科比", "詹姆斯", "摄像头",
    "空调","手机","电脑","耳机","充电器","键盘","鼠标","净水器"};

    private static TransportClient getClient() throws UnknownHostException {
        // 指定es集群名称
        Settings set = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        // 创建访问es集群的客户端
        TransportClient client = new PreBuiltTransportClient(set)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.147.130"), 9300));
        return client;
    }

    public static void main(String[] args) throws Exception {
        TransportClient client = getClient();
        BulkRequestBuilder builder = client.prepareBulk();
        Random r = new Random();
        Date time;
        for (int i = 1000000; i < 2000000; i++) {
            time = buidTime();
            builder.add(client.prepareIndex("hardware", "equipment", String.valueOf(i + 1008))
                    .setSource(buildDocument(String.valueOf(i + 1), r.nextInt(1000), r.nextInt(1000), buildName(), time, time)));
            System.out.println("........." + i + "...........");
        }
        BulkResponse response = builder.get();
        System.out.println(response.status());

    }

    private static String buildName() {
        StringBuilder sb = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < 7; i++) {
            sb.append(NAMES[random.nextInt(NAMES.length)]);
        }
        return sb.toString();
    }

    private static Date buidTime() {
        long time = System.currentTimeMillis();
        long ahour = 1000 * 60 * 60;

        int paincha = new Random().nextInt(1000 * 24);
        long createTime = time - ahour * paincha;

        Date date = new Date(createTime);
//		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//		String format = sdf.format(date);
        return date;
    }

    private static XContentBuilder buildDocument(String id, Integer enterCount, Integer outCount, String name,
                                                 Date createTime, Date updateTime) throws Exception {
        XContentBuilder doc = XContentFactory.jsonBuilder().startObject()
                .field("id", id)
                .field("name", name)
                .field("createTime", createTime).field("updateTime", updateTime).field("peopleIn", enterCount)
                .field("peopleOut", outCount).endObject();
        return doc;
    }

}
