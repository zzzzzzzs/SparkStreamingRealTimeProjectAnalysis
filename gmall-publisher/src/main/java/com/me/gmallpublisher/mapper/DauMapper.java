package com.me.gmallpublisher.mapper;

import org.apache.ibatis.annotations.Mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {
    //查询日活总数数据抽象方法，实现类在resources包下mapper中DauMapper.xml文件里的对应id
    public Integer selectDauTotal(String date);

    //查询日活分时数据抽象方法
    public List<Map> selectDauTotalHourMap(String date);
}
