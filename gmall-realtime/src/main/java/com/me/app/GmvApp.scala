package com.me.app

import com.alibaba.fastjson.JSON
import com.me.bean.OrderInfo
import com.me.constants.GmallConstants
import com.me.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object GmvApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GmvApp")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))

    //3.获取Kafka中GMALL_ORDER-topic中数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER,ssc)

    //4.将json数据转化为样例类
    val orderDStream: DStream[OrderInfo] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        //a.将数据转化为样例类
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

        //b.补全 date，hour字段
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)

        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)

        orderInfo
      })
    })


    //5.将数据写入Phoenix
    orderDStream.foreachRDD(rdd=>{
      rdd.saveToPhoenix("GMALL1116_ORDER_INFO",
        Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181")
      )
      })

    //6.开启任务
    ssc.start()
    ssc.awaitTermination()
  }

}
