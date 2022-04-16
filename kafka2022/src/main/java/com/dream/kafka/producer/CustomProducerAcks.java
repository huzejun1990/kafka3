package com.dream.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @Author : huzejun
 * @Date: 2022/4/15-20:01
 */
public class CustomProducerAcks {

    public static void main(String[] args) {

        // 0 配置
       Properties properties = new Properties();

       //连接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"master:9092,slave1:9092");

        //指定对应的序列化类型
//        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // acks
        properties.put(ProducerConfig.ACKS_CONFIG,"1");

        // 重试次数 retries,默认是 int 最大值 ，2147483647
        properties.put(ProducerConfig.RETRIES_CONFIG,3);

        //1、创建kafka生产者对象
        // "" hello
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //2、发送数据
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("first","dream" + i));
        }

        //3、关闭资源
        kafkaProducer.close();
    }
}
