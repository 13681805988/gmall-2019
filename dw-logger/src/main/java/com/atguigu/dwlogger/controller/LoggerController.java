package com.atguigu.dwlogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.constants.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

//@Controller
@RestController
@Slf4j
public class LoggerController {

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    @PostMapping("log")
//    @ResponseBody
    public String logger(@RequestParam("logString") String logStr){
//        System.out.println(logStr);

        //添加时间戳字段
        JSONObject jsonObject = JSON.parseObject(logStr);

        jsonObject.put("ts",System.currentTimeMillis());

         String jsonString = jsonObject.toString();

        log.info(jsonString);

        if(jsonString.contains("startup")){
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP,jsonString);
        }else {
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT,jsonString);
        }

        return "";
    }
}
