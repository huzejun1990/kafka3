package com.dream.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @Author : huzejun
 * @Date: 2022/4/21-18:05
 */
public class FlinkKafkaConsumer1 {
    public static void main(String[] args) throws Exception {

        //1 获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(3);

        // 2 创建一个消费者
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"master:9092,slave1:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test1");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("first", new SimpleStringSchema(), properties);

        //3 关联 消费者 和 flink流
        env.addSource(kafkaConsumer).print();

       // 4 执行
        env.execute();

    }
}
