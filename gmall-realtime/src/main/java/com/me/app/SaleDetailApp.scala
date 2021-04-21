package com.me.app

import java.util

import com.alibaba.fastjson.JSON
import com.me.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.me.constants.GmallConstants
import com.me.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer
//import org.json4s.native.Serialization
import collection.JavaConverters._

object SaleDetailApp {
    def main(args: Array[String]): Unit = {
        //1.创建SparkConf
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleDetailApp")

        //2.创建StreamingContext
        val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

        //3.获取Kafka中GMALL_ORDER和TOPIC_ORDER_DETAIL的topic中获取数据
        val orderInfoDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)
        val detailDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)

        // 打印从kafka中消费的数据
        // key没有值，只有value()有值
        //        orderInfoDStream.map(record => record.value()).print()
        //        detailDStream.map(record => record.value()).print()

        //4.将数据转化为样例类,并将数据转为kv形式，为了后面join的时候使用
        val idToinfoDStream: DStream[(String, OrderInfo)] = orderInfoDStream.mapPartitions(partition => {
            partition.map(record => {
                //a.将数据转化为样例类
                val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

                //b.补全 date，hour字段
                orderInfo.create_date = orderInfo.create_time.split(" ")(0)

                orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)

                (orderInfo.id, orderInfo)
            })
        })

        val idToDetailDStream: DStream[(String, OrderDetail)] = detailDStream.mapPartitions(partition => {
            partition.map(record => {
                val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
                (orderDetail.order_id, orderDetail)
            })
        })

        //5.双流join，将两条流join起来，将相同的相同key对应的所有元素连接在一起的(K,(V,W))的RDD
        val value: DStream[(String, (OrderInfo, OrderDetail))] = idToinfoDStream.join(idToDetailDStream)
        value.print()
        //    val fullJoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = idToinfoDStream.fullOuterJoin(idToDetailDStream)
        //
        //    fullJoinDStream.mapPartitions(partition => {
        //      //1.创建结果集合，用来存放能够关联上的数据
        //      val details: ListBuffer[SaleDetail] = new ListBuffer[SaleDetail]
        //      val jedisClient: Jedis = new Jedis()
        //
        //      //2.操作数据
        //      partition.foreach { case (orderId, (infoOpt, detailOpt)) => {
        //
        //        //3.判断orderInfo数据是否存在
        //        if (infoOpt.isDefined) {
        //          //orderInfo存在,获取orderinfo数据
        //          val orderInfo: OrderInfo = infoOpt.get
        //          //4.判断orderDetail数据是否存在
        //          if (detailOpt.isDefined) {
        //            //orderDetail存在
        //            val orderDetail: OrderDetail = detailOpt.get
        //            details += new SaleDetail(orderInfo, orderDetail)
        //          }
        //          //5.无论如何都要将orderInfo写入redis
        //          val infoRedisKey: String = "orderInfo:" + orderId
        //          val detailRedisKey: String = "orderDetail:" + orderId
        //          //          JSON.toJSONString(orderInfo)
        //          //将样例类转化为json字符串
        //          implicit val formats = org.json4s.DefaultFormats
        //          val orderInfoJson: String = Serialization.write(orderInfo)
        //
        //          jedisClient.set(infoRedisKey, orderInfoJson)
        //          jedisClient.expire(infoRedisKey, 10)
        //
        //          //6.去orderDetail缓存中查取是有能关联上的数据
        //          val orderDetails: util.Set[String] = jedisClient.smembers(detailRedisKey)
        //          for (elem <- orderDetails.asScala) {
        //            //将查出来的orderDetail字符串转为样例类
        //            val orderDetail: OrderDetail = JSON.parseObject(elem, classOf[OrderDetail])
        //            details += new SaleDetail(orderInfo, orderDetail)
        //          }
        //        }
        //      }
        //      }
        //      jedisClient.close()
        //      partition
        //    })


        ssc.start()
        ssc.awaitTermination()

    }

}
