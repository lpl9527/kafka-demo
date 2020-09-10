package com.lpl;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.swing.*;
import java.util.Properties;

/**
 * 消息生产者
 */
public class ProducerFastStart {

    private static final String brokerList = "129.211.171.112:9092";    //kafka集群列表

    private static final String topic = "Wechat";   //主题

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("bootstrap.servers", brokerList);
        //构造生产者
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        //构造消息对象
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "Hello Kafka...");
        //发送消息
        try {
            producer.send(record);
        }catch (Exception e) {
            e.printStackTrace();
        }
        //关闭生产者客户端连接
        producer.close();
    }
}
