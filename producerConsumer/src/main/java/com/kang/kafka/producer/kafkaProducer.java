package com.kang.kafka.producer;

import com.kang.kafka.common.PropertiesVal;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @Title
 * @Description
 * @Date 2016/12/28 1:33
 * @Author Healthy
 * @Version 2.0
 */
@Deprecated
public class kafkaProducer  extends  Thread {

    private String topic;

    public kafkaProducer(String topic){
        super();
        this.topic = topic;
    }

    public void run() {
        Producer producer = createProducer();
        int i=0;
        while(true){
            producer.send(new KeyedMessage<Integer, String>(topic, "message: " + i++));
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private Producer createProducer() {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", PropertiesVal.testZk);
        properties.put("serializer.class", StringEncoder.class.getName());
        properties.put("metadata.broker.list",PropertiesVal.testBrokers);// 声明kafka broker
        return new Producer<Integer, String>(new ProducerConfig(properties));
    }

    public static void main(String[] args) {
        new kafkaProducer(PropertiesVal.tesTopicName).start();// 使用kafka集群中创建好的主题 test
    }
}
