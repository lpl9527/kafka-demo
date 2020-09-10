package com.lpl;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 生产者拦截器
 */
public class ProducerInterceptorPrefix implements ProducerInterceptor<String, String> {

    private volatile long sendSuccess = 0;      //记录发送成功数
    private volatile long sendFailure = 0;      //记录发送失败数

    /**
     * 处理发送的消息
     * @param producerRecord
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        //为消息加上前缀
        String modifiedValue = "LPL Kafka..." + producerRecord.value();
        return new ProducerRecord<>(producerRecord.topic(), producerRecord.partition(), producerRecord.timestamp(),
                producerRecord.key(), modifiedValue, producerRecord.headers());
    }

    /**
     * 此消息应答方法优先于设定的Callable之前执行
     * @param recordMetadata
     * @param e
     */
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        //统计成功失败数
        if (null == e) {
            sendSuccess++;
        }else {
            sendFailure++;
        }
    }

    /**
     * 关闭连接的方法
     */
    @Override
    public void close() {
        //关闭生产时统计消息发送成功率
        double successRatio = (double)sendSuccess / (sendSuccess + sendFailure);
        System.out.println("【INFO】消息发送成功率=" + String.format("%.3f", successRatio * 100) + "%");
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
