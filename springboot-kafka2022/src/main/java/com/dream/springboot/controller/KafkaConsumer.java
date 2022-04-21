package com.dream.springboot.controller;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * @Author : huzejun
 * @Date: 2022/4/21-20:02
 */
@Configuration
public class KafkaConsumer {

    @KafkaListener(topics = "first")
    public void consumerTopic(String msg){
        System.out.println("收到消息： " + msg);
    }
}
