package com.me.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {

    //获取GMV总数抽象方法
    public Double selectOrderAmountTotal(String date);

    //获取GMV分时数据抽象方法
    public List<Map> selectOrderAmountHourMap(String date);
}
