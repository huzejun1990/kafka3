package com.dream.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @Author : huzejun
 * @Date: 2022/4/21-17:33
 */
public class FlinkKafkaProducer1 {

    public static void main(String[] args) throws Exception {
        // 1 获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(3);

        // 2 准备数据源
        ArrayList<String> worldlist = new ArrayList<>();
        worldlist.add("hello");
        worldlist.add("dream");
        DataStreamSource<String> stream = env.fromCollection(worldlist);

        // 创建一个生产者
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"master:9092,slave1:9092");

        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>("first", new SimpleStringSchema(), properties);

        // 3 添加数据源 kafka生产者
        stream.addSink(kafkaProducer);

        // 4 执行代码
        env.execute();

    }


}
