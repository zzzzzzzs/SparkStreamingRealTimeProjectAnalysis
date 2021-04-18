package com.me.gmallpublisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.me.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class Controller {

    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String realtimeTotal(@RequestParam("date") String date) {
        //1.创建List<Map>集合用来存放结果数据
        ArrayList<Map> result = new ArrayList<Map>();

        //2.从service层获取处理好的数据
        Integer dauTotal = publisherService.getDauTotal(date);

        //3.将数据封装到结果集合中
        //3.1将新增日活数据封装到map中
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        //3.2将新增设备的数据封装到map中
        HashMap<String, Object> devMap = new HashMap<>();
        devMap.put("id", "new_mid");
        devMap.put("name", "新增设备");
        devMap.put("value", 233);

        //3.3将map集合存放到List集合中
        result.add(dauMap);
        result.add(devMap);

        //4.返回
        return JSONObject.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String realtimeHours(@RequestParam("id") String id,
                                @RequestParam("date") String date
    ) {
        //0.跟据传进来的日期获取昨天的日期
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();

        //1.获取service层数据
        Map<String, Long> todayMap = publisherService.getDauHour(date);
        //1.2查询昨天的数据
        Map<String, Long> yesterdayMap = publisherService.getDauHour(yesterday);

        //2.创建map集合用来存放结果数据
        HashMap<String, Map> result = new HashMap<>();

        //3.往结果集合中封装数据
        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);

        return JSONObject.toJSONString(result);
    }
}
