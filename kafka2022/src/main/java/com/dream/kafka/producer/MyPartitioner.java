package com.dream.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @Author : huzejun
 * @Date: 2022/4/15-23:16
 */
public class MyPartitioner implements Partitioner {
    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {

        //获取数据 drean hello
        String msgValues = o1.toString();

        int partition;

        if (msgValues.contains("dream")){
            partition = 0;
        }else {
            partition = 1;
        }
        return partition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
