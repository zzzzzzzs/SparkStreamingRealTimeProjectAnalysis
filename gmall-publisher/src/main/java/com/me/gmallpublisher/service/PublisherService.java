package com.me.gmallpublisher.service;

import java.util.Map;

public interface PublisherService {
    //日活数据总数抽象方法 =>service
    public Integer getDauTotal(String date);

    //日活数据分时抽象方法
    public Map<String, Long> getDauHour(String date);
}
