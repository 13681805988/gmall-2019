package com.atguigu.dwpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.dwpublisher.service.DauService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    DauService dauService;

    //请求总数统计
    @GetMapping("realtime-total")
    public String getRealTimeTotal(@RequestParam("date") String todaydate){
        int total = dauService.getTotal(todaydate);

        //新建存放字段的集合 格式为
        ArrayList<Map> toJsonMap = new ArrayList<>();
        //创建集合用于存放新增日活
        HashMap<String, Object> newDau = new HashMap<>();
        newDau.put("id","dau");
        newDau.put("name","新增日活");
        newDau.put("value",total);
        //创建集合用于存放新增设备
        HashMap<String, Object> newMid = new HashMap<>();
        newMid.put("id","new_mid");
        newMid.put("name","新增设备");
        newMid.put("value","233");

        toJsonMap.add(newDau);
        toJsonMap.add(newMid);

        String s = JSON.toJSONString(toJsonMap);

        return  s;
    }




    //请求分时统计的页面
    @GetMapping("realtime-hours")
    public String getHours(@RequestParam("id") String id,
                           @RequestParam("date") String date) throws ParseException {
        HashMap<String, Map> json = new HashMap<>();
        Map todayHourCount = dauService.getHourCount(date);
        SimpleDateFormat sfd = new SimpleDateFormat("yyyy-MM-dd");
        Calendar ins = Calendar.getInstance();
        ins.setTime(sfd.parse(date));
        ins.add(Calendar.DAY_OF_MONTH,-1);

        String yesterdayTime = sfd.format(new Date(ins.getTimeInMillis()));
        Map yesterdayHourCount = dauService.getHourCount(yesterdayTime);

        //存放昨日以及今日数据至集合
        json.put("today",todayHourCount);
        json.put("yesterday",yesterdayHourCount);
        String s = JSON.toJSONString(json);

        return s;
    }


}
