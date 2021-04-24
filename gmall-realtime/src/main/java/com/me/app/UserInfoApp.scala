package com.me.app

import com.alibaba.fastjson.JSON
import com.me.bean.UserInfo
import com.me.constants.GmallConstants
import com.me.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import redis.clients.jedis.Jedis

object UserInfoApp {
    def main(args: Array[String]): Unit = {
        //1.创建SparkConf
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UserInfoApp")

        //2.创建StreamingContext
        val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

        //3.获取Kafka中TOPIC_USER_INFO中数据
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER, ssc)

        //4.将userInfo数据写入redis
        kafkaDStream.foreachRDD(rdd => {
            rdd.foreachPartition(partition => {
                val jedisClient: Jedis = new Jedis("hadoop102", 6379)
                partition.foreach(redcord => {
                    //为了拿到userId，因此将数据转为样例类
                    val userInfo: UserInfo = JSON.parseObject(redcord.value(), classOf[UserInfo])
                    val userInfoRedisKey: String = "userInfo:" + userInfo.id
                    jedisClient.set(userInfoRedisKey, redcord.value())
                })
                jedisClient.close()
            })
        })

        kafkaDStream.map(record => record.value()).print()

        ssc.start()
        ssc.awaitTermination()
    }

}
