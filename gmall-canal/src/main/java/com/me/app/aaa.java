package com.me.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.me.constants.GmallConstants;
import com.me.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

public class aaa {
    //TODO
    // 流程：

    //  CanalEntry.EntryType枚举类有5种类型：
    //  TRANSACTIONBEGIN、ROWDATA、TRANSACTIONEND、HEARTBEAT、GTIDLOG
    public static void main(String[] args) throws InvalidProtocolBufferException, InterruptedException {
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(//1.获取canal连接对象
                new InetSocketAddress("hadoop102", 11111), "example", "", "");
        while (true) {
            canalConnector.connect();//2.获取连接
            canalConnector.subscribe("gmall1116");//3.指定要监控的数据库  注意用gmall*会识别不了，因这里用的通配符，*前面需加.
            Message message = canalConnector.get(100);//4.获取Message
            List<CanalEntry.Entry> entries = message.getEntries();
            if (entries.size() <= 0) {
                System.out.println("没有数据，休息一会");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                System.out.println("有数据");
                for (CanalEntry.Entry entry : entries) {
                    String tableName = entry.getHeader().getTableName();// 获取表名
                    CanalEntry.EntryType entryType = entry.getEntryType();//获取Entry类型
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {//判断entryType是否为ROWDATA 防止空指针异常
                        ByteString storeValue = entry.getStoreValue();//获取序列化数据
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);//反序列化
                        CanalEntry.EventType eventType = rowChange.getEventType();//获取事件类型
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();// 获取具体的数据
                        handler(tableName, eventType, rowDatasList);//根据条件获取数据
                    }
                }
            }
        }
    }

    //根据不同业务的类型发送不同主题
    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) throws InterruptedException {
        //需求2：获取订单表的新增数据
        //因求GMV需用新增的订单量。（不能是修改的）
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType))
            saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER);
            //需求4：获取订单表的新增数据
        else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER_DETAIL);//获取订单详情表的新增数据
        } else if ("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType) || CanalEntry.EventType.UPDATE.equals(eventType))) {
            saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_USER);//获取用户表的新增及变化数据
        }
    }

    //ctrl+alt+m 将某一块代码放入方法中
    private static void saveToKafka(List<CanalEntry.RowData> rowDatasList, String topic) throws InterruptedException {
        for (CanalEntry.RowData rowData : rowDatasList) {
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();//获取存放列的集合
            JSONObject jsonObject = new JSONObject();//获取每个列
            for (CanalEntry.Column column : afterColumnsList) {
                jsonObject.put(column.getName(), column.getValue());
            }
            System.out.println(jsonObject.toString());
            //Thread.sleep(new Random().nextInt());//模拟网络延迟
            MyKafkaSender.send(topic, jsonObject.toString());
        }
    }
}

