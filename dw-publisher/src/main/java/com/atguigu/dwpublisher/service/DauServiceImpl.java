package com.atguigu.dwpublisher.service;


import com.atguigu.dwpublisher.mapper.DauMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DauServiceImpl implements DauService{

    @Autowired
    DauMapper dauMapper;

    @Override
    public int getTotal(String date) {
        return dauMapper.getTotal(date);
    }

    @Override
    public Map getHourCount(String date) {

        List<Map> mapperHourCount = dauMapper.getMapperHourCount(date);
        HashMap<String, Long> stringLongHashMap = new HashMap<>();
        for (Map map : mapperHourCount) {
            stringLongHashMap.put((String) map.get("LH"),(Long) map.get("CT"));
        }

        return stringLongHashMap;
    }
}
