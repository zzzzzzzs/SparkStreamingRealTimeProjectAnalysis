package com.me.gmallpublisher.service.impl;

import com.me.gmallpublisher.mapper.DauMapper;
import com.me.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map<String, Long> getDauHour(String date) {
        //1.获取数据 => 获取mapper传过来的数据
        List<Map> mapList = dauMapper.selectDauTotalHourMap(date);

        //2.创建Map集合存放结果数据
        HashMap<String, Long> result = new HashMap<>();

        //3.解析mapper传过来的数据，并封装到新的Map集合中
        for (Map map : mapList) {
            result.put(map.get("LH").toString(), (Long) map.get("CT"));
        }
        return result;
    }
}
