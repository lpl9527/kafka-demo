package com.lpl;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.swing.*;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 消息生产者
 */
public class ProducerFastStart {

    private static final String brokerList = "129.211.171.112:9092";    //kafka集群列表

    private static final String topic = "Wechat";   //主题

    /**
     * 初始化生产者客户端参数
     */
    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
        /*
            KafkaProducer中一般发生两种异常：
                可重试的异常：
                    NetworkException，可能是网络瞬时故障引起的异常，可通过重试解决。
                    LeaderNotAvailableException，表示leader副本不可用异常，通常发生在leader副本下线而新的leader副本选举完成之前，重试也可恢复。
                不可重试的异常：
                    RecordTooLargeException，所发送的消息太大，KafkaProducer不会进行重试，直接抛出异常。
            对于可重试异常，如果配置了retries参数，只要在规定的次数（默认为0）内自动恢复了就不会抛出异常。
        */
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);
        //指定自定义的生产者拦截器
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ProducerInterceptorPrefix.class.getName());

        return properties;
    }

    public static void main(String[] args) {
        Properties props = initConfig();
        //构造生产者
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        int i = 0;
        while (i < 100) {
            //发送消息，三种发送方式：发后即忘、同步、异步。
            try {
                //构造消息对象
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, "生产的第" + i++ + "条消息！");

                //1.发送即忘方式，性能最高，不保证Kafka服务端是否收到消息，可能造成消息的丢失。
                //producer.send(record);

                //2.异步方式，实际上send()方法本身就是异步的，通过制定一个Callback回调函数来实现异步的发送确认。
            /*producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {  //发送结果两个参数必有一个为null
                    if (null != e) {
                        //消息重发或者日志记录分析等逻辑处理
                        e.printStackTrace();
                    } else {
                        System.out.println("发送结果：" + recordMetadata.topic() + "--" + recordMetadata.partition() + ":" + recordMetadata.offset());
                    }
                }
            });*/

                //3.同步方式，send()返回的Future对象可以使调用方稍后获得发送的结果。可靠性高，要么发送成功，要么发生异常。同步发送是在发送之后直接
                //链式调用了get()方法来阻塞等待Kafka的相应，直到消息发送成功或者发生异常。
                //RecordMetadata recordMetadata = producer.send(record).get();
                //也可不进行链式调用get()方法实现同步发送。
                //Future对象表示一个任务的生命周期，并提供了相应的方法判断任务是否已经完成或取消，以及获取任务的结果或者取消任务等。这样我们就可以
                //通过get(long timeout, TimeUnit unit)方法实现超时阻塞控制。
                Future<RecordMetadata> future = producer.send(record);
                RecordMetadata recordMetadata = future.get();

                System.out.println("发送结果：" + recordMetadata.topic() + "--" + recordMetadata.partition() + ":" + recordMetadata.offset());

            }catch (Exception e) {
                e.printStackTrace();
            }
        }

        //关闭生产者客户端连接
        producer.close();
    }
}
