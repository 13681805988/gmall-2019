package com.atguigu.dwpublisher.service;

import java.util.Map;

public interface GmvService {
    //获取某一天GMV总金额 即求sum
    public Double serviceGetTotal(String date);
    //获取当天分时统计总金额
    public Map serviceGetHourTotal(String date);
}
