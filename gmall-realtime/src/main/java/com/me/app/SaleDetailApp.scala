package com.me.app

import java.util

import com.alibaba.fastjson.JSON
import com.me.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.me.constants.GmallConstants
import com.me.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer
import org.json4s.native.Serialization

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
        //        orderInfoDStream.map(ee=>ee.value()).print()
        //        detailDStream.map(ee=>ee.value()).print()

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
        // 在理想情况下用join可以，但是由于网络等因素不可能在理想的情况下将所有的数据join在一起
        //        val joinDStream: DStream[(String, (OrderInfo, OrderDetail))] = idToinfoDStream.join(idToDetailDStream)
        //        joinDStream.print()

        val fullJoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = idToinfoDStream.fullOuterJoin(idToDetailDStream)
        //6.利用缓存的方式，将延迟的数据join起来
        val noUserSaleDetailDStream: DStream[SaleDetail] = fullJoinDStream.mapPartitions(partition => {
            //1.创建结果集合，用来存放能够关联上的数据
            val details: ListBuffer[SaleDetail] = new ListBuffer[SaleDetail]
            val jedisClient: Jedis = new Jedis("hadoop102", 6379)

            //2.操作数据
            partition.foreach {
                case (orderId, (infoOpt, detailOpt)) => {
                    val infoRedisKey: String = "orderInfo:" + orderId
                    val detailRedisKey: String = "orderDetail:" + orderId
                    //3.判断orderInfo数据是否存在
                    if (infoOpt.isDefined) {
                        //orderInfo存在,获取orderinfo数据
                        val orderInfo: OrderInfo = infoOpt.get
                        //4.判断orderDetail数据是否存在
                        if (detailOpt.isDefined) {
                            //orderDetail存在
                            val orderDetail: OrderDetail = detailOpt.get
                            details += new SaleDetail(orderInfo, orderDetail)
                        }
                        //5.无论如何都要将orderInfo写入redis
                        //          JSON.toJSONString(orderInfo)
                        //将样例类转化为json字符串
                        implicit val formats = org.json4s.DefaultFormats
                        val orderInfoJson: String = Serialization.write(orderInfo)
                        jedisClient.set(infoRedisKey, orderInfoJson)
                        jedisClient.expire(infoRedisKey, 100)

                        //6.去orderDetail缓存中查取是否有能关联上的数据
                        if (jedisClient.exists(detailRedisKey)) {
                            val orderDetails: util.Set[String] = jedisClient.smembers(detailRedisKey)
                            for (elem <- orderDetails.asScala) {
                                //将查出来的orderDetail字符串转为样例类
                                val orderDetail: OrderDetail = JSON.parseObject(elem, classOf[OrderDetail])
                                details += new SaleDetail(orderInfo, orderDetail)
                            }
                        }
                    } else {
                        //orderInfo不存在
                        if (detailOpt.isDefined) {
                            //orderDetail存在,查对方缓存
                            val orderDetail: OrderDetail = detailOpt.get
                            //1.去对方缓存中查是否有对应的orderInfo
                            //1.1先判断一下redis中是否有对应的key
                            if (jedisClient.exists(infoRedisKey)) {
                                //有对应的key
                                //1.2获取key所对应的数据->orderInfo
                                val orderInfoStr: String = jedisClient.get(infoRedisKey)
                                //1.3将查过来的orderInfo字符串类型的数据转为样例类
                                val orderInfo: OrderInfo = JSON.parseObject(orderInfoStr, classOf[OrderInfo])
                                details += new SaleDetail(orderInfo, orderDetail)
                            } else {
                                //2.对方orderInfo缓存中没有对应的key,则将自己写入orderDetail缓存
                                implicit val formats = org.json4s.DefaultFormats
                                val orderDetailJson: String = Serialization.write(orderDetail)
                                jedisClient.sadd(detailRedisKey, orderDetailJson)
                                //3.对orderDetail数据设置过期时间
                                jedisClient.expire(detailRedisKey, 100)
                            }
                        }
                    }
                }
            }
            jedisClient.close()
            details.toIterator
        })
        //    noUserSaleDetailDStream.print()

        //                //7.查UserInfo缓存补全用户信息
        //                val saleDetailDStream: DStream[SaleDetail] = noUserSaleDetailDStream.mapPartitions(partition => {
        //                    //1.创建Redis连接
        //                    val jedisClient: Jedis = new Jedis("hadoop102", 6379)
        //                    val details: Iterator[SaleDetail] = partition.map(saleDetail => {
        //                        //2.根据redisKey获取数据
        //                        val userInfoRedisKey: String = "userInfo:" + saleDetail.user_id
        //                        val userInfoStr: String = jedisClient.get(userInfoRedisKey)
        //                        //3.为了使用SaleDetail样例类中的方法，因此将查出来的字符串转为样例类
        //                        val userInfo: UserInfo = JSON.parseObject(userInfoStr, classOf[UserInfo])
        //                        saleDetail.mergeUserInfo(userInfo)
        //                        saleDetail
        //                    })
        //                    jedisClient.close()
        //                    details
        //                })
        //                saleDetailDStream.print()

        //                //8.将数据写入ES
        //                saleDetailDStream.foreachRDD(rdd => {
        //                    rdd.foreachPartition(partition => {
        //                        val list: List[(String, SaleDetail)] = partition.toList.map(log => {
        //                            (log.order_detail_id, log)
        //                        })
        //                        MyEsUtil.insertBulk(GmallConstants.ES_DETAIL_INDEXNAME + System.currentTimeMillis() / 1000 / 60 / 60 / 24, list)
        //                    })
        //                })

        //    orderInfoDStream.map(record=>record.value()).print()
        //    detailDStream.map(record=>record.value()).print()

        ssc.start()
        ssc.awaitTermination()


    }

}
