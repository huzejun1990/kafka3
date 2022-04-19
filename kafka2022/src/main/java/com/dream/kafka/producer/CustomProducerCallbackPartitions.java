package com.dream.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @Author : huzejun
 * @Date: 2022/4/15-20:01
 */
public class CustomProducerCallbackPartitions {

    public static void main(String[] args) throws InterruptedException {

        // 0 配置
       Properties properties = new Properties();

       //连接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"master:9092,slave1:9092");

        //指定对应的序列化类型
//        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // 关联自定义分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.dream.kafka.producer.MyPartitioner");

        //1、创建kafka生产者对象
        // "" hello
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //2、发送数据
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("first", 1,"","hello" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                    if (e == null) {
                        System.out.println("主题： "+recordMetadata.topic() + "  分区： "+ recordMetadata.partition());
                    }
                    
                }
            });

            Thread.sleep(2);
        }

        //3、关闭资源
        kafkaProducer.close();
    }
}
