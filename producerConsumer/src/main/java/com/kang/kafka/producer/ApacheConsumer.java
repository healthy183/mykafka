package com.kang.kafka.producer;

import com.kang.kafka.common.PropertiesVal;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

/**
 * @Title
 * @Description
 * @Date 2017/1/9 17:22
 * @Author Healthy
 * @Version 2.0
 */
@Slf4j
public class ApacheConsumer {

    public static void main(String[] args) {
        //consumeMsgBalancing();
        //consumerMsgcustom();
        consumerMsgcustomBegin();
    }


   /* 在测试中我发现，如果用手工指定partition的方法拉取消息，
   不知为何kafka的自动提交offset机制会失效，
   必须使用手动方式才能正确提交已消费的消息offset。
    */
   public static void consumeMsgBalancing()  {
        Properties properties =  PropertiesVal.getConsumer();
        KafkaConsumer consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(PropertiesVal.topicName));
        while(true){
            try{
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for(ConsumerRecord<String, String> record : records){
                log.info("fetched from partition [{}],offset:[{}],message:[{}]",
                        record.partition(),record.offset(),record.value());
            }
        }
    }


    public static void consumerMsgcustom(){
        Properties properties =  PropertiesVal.getConsumer();
        KafkaConsumer consumer = new KafkaConsumer<String, String>(properties);
        TopicPartition partition0 = new TopicPartition(PropertiesVal.topicName, 0);
        TopicPartition partition1 = new TopicPartition(PropertiesVal.topicName, 1);
        consumer.assign(Arrays.asList(partition0,partition1));

        while(true){
            try{
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            ConsumerRecords<String, String> records = consumer.poll(1000);
                for(ConsumerRecord<String, String> record : records){
                    log.info("fetched from partition [{}],offset:[{}],message:[{}]",
                        record.partition(),record.offset(),record.value());
            }
            consumer.commitSync();
        }
    }


    @Deprecated
    public static void consumerMsgcustomBegin(){
        Properties properties =  PropertiesVal.getConsumer();
        KafkaConsumer consumer = new KafkaConsumer<String, String>(properties);
        TopicPartition partition0 = new TopicPartition(PropertiesVal.topicName, 0);
        TopicPartition partition1 = new TopicPartition(PropertiesVal.topicName, 1);
        consumer.subscribe(Arrays.asList(PropertiesVal.topicName));
        //consumer.seekToBeginning(Arrays.asList(partition0,partition1));
        consumer.seekToBeginning(new ArrayList<TopicPartition>());
        while(true){
            try{
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for(ConsumerRecord<String, String> record : records){
                log.info("fetched from partition [{}],offset:[{}],message:[{}]",
                        record.partition(),record.offset(),record.value());
            }
            consumer.commitSync();
        }
    }




}
