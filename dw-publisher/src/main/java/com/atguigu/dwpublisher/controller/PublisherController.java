package com.atguigu.dwpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.dwpublisher.service.DauService;
import com.atguigu.dwpublisher.service.GmvService;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
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

    @Autowired
    GmvService gmvService;

    //请求总数统计
    @GetMapping("realtime-total")
    public String getRealTimeTotal(@RequestParam("date") String todaydate){
        //获取新增
        int total = dauService.getTotal(todaydate);
        //获取今日gmv总额
        Double gmvTotal = gmvService.serviceGetTotal(todaydate);


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

        //创建Map集合存放GMV数据
        HashMap<String, Object> newGmv = new HashMap<>();
        newGmv.put("id", "order_amount");
        newGmv.put("name", "新增交易额");
        newGmv.put("value", gmvTotal);

        toJsonMap.add(newDau);
        toJsonMap.add(newMid);
        toJsonMap.add(newGmv);


        String s = JSON.toJSONString(toJsonMap);

        return  s;
    }




    //请求分时统计的页面
    @GetMapping("realtime-hours")
    public String getHours(@RequestParam("id") String id,
                           @RequestParam("date") String date) throws ParseException {
        HashMap<String, Map> json = new HashMap<>();
        String yesterday = getYesterday(date);

        Map todayMap=null;
        Map yesterdayMap=null;

        if("dau".equals(id)){
             todayMap = dauService.getHourCount(date);
             yesterdayMap = dauService.getHourCount(yesterday);

        }else if("order_amount".equals(id)){
             todayMap = gmvService.serviceGetHourTotal(date);
             yesterdayMap = gmvService.serviceGetHourTotal(yesterday);
        }


        //存放昨日以及今日数据至集合
        json.put("today",todayMap);
        json.put("yesterday",yesterdayMap);
        String s = JSON.toJSONString(json);

        return s;
    }



    private static String getYesterday(String date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar instance = Calendar.getInstance();
        try {
            instance.setTime(sdf.parse(date));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        //将当天时间减一
        instance.add(Calendar.DAY_OF_MONTH, -1);
        //2020-02-18
        return sdf.format(new Date(instance.getTimeInMillis()));
    }
}
