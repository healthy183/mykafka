package com.kang.kafka.producer;

import com.google.common.base.Throwables;
import com.kang.kafka.common.PropertiesVal;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @Title
 * @Description
 * @Date 2017/1/3 11:26
 * @Author Healthy
 * @Version 2.0
 */
@Slf4j
public class ApacheProducer {

    public static void main(String[] args) {
        System.out.println("begin produce");
        connectionKafka();
        System.out.println("finish produce");
    }

    public static void connectionKafka() {

        Producer<String, String> producer = new KafkaProducer<>(PropertiesVal.getProducer());
        for (int i = 0; i < 10; i++) {
            //producer.send(new ProducerRecord<String, String>(PropertiesVal.topicName, Integer.toString(i), Integer.toString(i)));
            String msg  = "msg is "+i;
            ProducerRecord producerRecord = new ProducerRecord<String, String>(PropertiesVal.topicName, Integer.toString(i),msg);
            producer.send(producerRecord,(RecordMetadata metadata, Exception e)->{
                if(e != null){
                    log.info(Throwables.getStackTraceAsString(e));
                }else{
                    log.info("message[{}] send  to partition[{}],offset[{}] completely!",msg,metadata.partition(),metadata.offset());
                }
            });
        }
        producer.close();
    }


}
