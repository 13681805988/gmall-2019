package com.atguigu.dwpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface GmvMapper {
    //获取某一天GMV总金额 即求sum
    public Double selectOrderAmountTotal(String date);

    //获取当天分时统计总金额
    public List<Map> selectOrderAmountHourMap(String date);
}
