package com.lh.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 10:47 2022/8/5
 */
@Slf4j
@RestController
public class LoggerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("test1")
    public String test1(){
        System.out.println("111");
        return "success";
    }

    @RequestMapping("test2")
    public String test2(@RequestParam("name") String name,
                        @RequestParam("age") int age){
        System.out.println(name + ":" + age);
        return "success";
    }


    @RequestMapping("applog")
    public String getLogger(@RequestParam("param") String jsonStr){
        System.out.println("------" + jsonStr);
        log.info(jsonStr);
        kafkaTemplate.send("ods_base_log", jsonStr);
        return "success";
    }


}
