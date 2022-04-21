package com.dream.springboot.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author : huzejun
 * @Date: 2022/4/21-19:16
 */
@RestController
public class ProducerController {

    @Autowired
    KafkaTemplate<String, String> kafka;

    @RequestMapping("/dream")
    public String data(String msg){
        // 能过 kafka发送出去
        kafka.send("first",msg);
        System.out.println("========/dream========");

        return "ok";
    }

}
