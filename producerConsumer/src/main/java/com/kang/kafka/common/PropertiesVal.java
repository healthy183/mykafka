package com.kang.kafka.common;

import lombok.Data;

import java.util.Properties;

/**
 * @Title
 * @Description
 * @Date 2016/12/28 1:38
 * @Author Healthy
 * @Version 2.0
 */
@Data
public class PropertiesVal {
    public static String zk = "192.168.88.134:2181,192.168.88.134:2182,192.168.88.134:2183";

    public static String  brokers = "192.168.88.134:9092,192.168.88.134:9093,192.168.88.134:9094";

    public static String topicName = "tripleDoubles";

    public static String  testZk = "192.168.88.134:2181";

    public static String  testBrokers = "192.168.88.134:9092";

    public static String tesTopicName = "test";

    public static Properties getProducer(){
        Properties props = new Properties();
        props.put("bootstrap.servers",PropertiesVal.brokers);
        props.put("metadata.broker.list", PropertiesVal.brokers);// 声明kafka broker
        /***集群完整确认，Leader会等待所有in-sync的follower节点都确认收到消息后，再返回确认信息
         我们可以根据消息的重要程度，设置不同的确认模式。默认为1**/
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }


    public static Properties getConsumer(){
        Properties props = new Properties();
        props.put("bootstrap.servers",PropertiesVal.brokers);
        props.put("metadata.broker.list", PropertiesVal.brokers);// 声明kafka broker
        props.put("group.id", "1");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

}
