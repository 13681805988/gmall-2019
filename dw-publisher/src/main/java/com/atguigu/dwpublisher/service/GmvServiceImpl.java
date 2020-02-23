package com.atguigu.dwpublisher.service;

import com.atguigu.dwpublisher.mapper.GmvMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class GmvServiceImpl implements GmvService{

    @Autowired
    GmvMapper gmvMapper;


    @Override
    public Double serviceGetTotal(String date) {
        return gmvMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map serviceGetHourTotal(String date) {
        //含有多个Map集合的list
        List<Map> list = gmvMapper.selectOrderAmountHourMap(date);
        HashMap<String, Double> myMap = new HashMap<>();
        for (Map map : list) {
            myMap.put((String)map.get("CREATE_HOUR"),(Double)map.get("SUM_AMOUNT"));
        }
        return myMap;
    }
}
