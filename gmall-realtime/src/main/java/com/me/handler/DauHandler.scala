package com.me.handler

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

import com.me.bean.StartUpLog
import com.me.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {
    /**
     * 批次内去重
     *
     * @param filterByRedisDStream
     */
    def filterByMid(filterByRedisDStream: DStream[StartUpLog]) = {
        //1.将数据转为k,v （(mid,logdate),StartUpLog）
        val midAndDateToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedisDStream.map(log => {
            ((log.mid, log.logDate), log)
        })
        //2.使用groupbykey将相同key的数据聚和到一起
        val midAndDateToLogIterDStream: DStream[((String, String), Iterable[StartUpLog])] = midAndDateToLogDStream.groupByKey()
        //3.对相同key的数据按照时间排序，并取出第一条
        val sortWithTsDStream: DStream[((String, String), List[StartUpLog])] = midAndDateToLogIterDStream.mapValues(iter => {
            iter.toList.sortWith(_.ts < _.ts).take(1)
        })
        //4.将value的list集合打散并返回
        val value: DStream[StartUpLog] = sortWithTsDStream.flatMap(_._2)

        value
    }

    /**
     * 利用redis进行跨批次去重
     *
     * @param startUpLogDStream
     */
    def filterByRedis(startUpLogDStream: DStream[StartUpLog], sc: SparkContext) = {
//        对数据进行去重操作(方案一)
//            val value: DStream[StartUpLog] = startUpLogDStream.filter(log => {
//              //a.获取redis连接
//              val jedisClient: Jedis = RedisUtil.getJedisClient
//              //b.rediskey
//              val redisKey: String = "DAU:" + log.logDate
//              //c.拿本批次的mid去redis中对比数据，判断是否存在
//              val boolean: lang.Boolean = jedisClient.sismember(redisKey, log.mid)
//                //关闭连接
//              jedisClient.close()
//              !boolean
//            })
//            value
//        对数据进行去重操作(方案二:在分区下获取连接以减少连接次数)
//            val value1: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partition => {
//              //a.获取redis连接
//              val jedisClient: Jedis = RedisUtil.getJedisClient
//              println("123")
//              val logs: Iterator[StartUpLog] = partition.filter(log => {
//                //b.rediskey
//                val redisKey: String = "DAU:" + log.logDate
//                //c.拿本批次的mid去redis中对比数据，判断是否存在
//                val boolean: lang.Boolean = jedisClient.sismember(redisKey, log.mid)
//                println("abc")
//                !boolean
//              })
//              //关闭连接
//              jedisClient.close()
//              logs
//            })
//            value1
        //对数据进行去重操作(方案三:在每个批次内获取一次连接) ？？？ 为什么要每个批次获取一次连接，为什么不能长连接
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val value3: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
            //1.获取redis连接（driver端）
            val jedisClient: Jedis = RedisUtil.getJedisClient

            //2.从redis中获取数据(driver)。现在无法获取log.logDate，所以使用当前系统的时间。
            val redisKey: String = "DAU:" + sdf.format(new Date(System.currentTimeMillis()))
            val mids: util.Set[String] = jedisClient.smembers(redisKey)
            //3.将driver获取的redis中的数据利用广播变量将其广播到executor端，方式序列化问题
            val midsBC: Broadcast[util.Set[String]] = sc.broadcast(mids)

            val rddBC: RDD[StartUpLog] = rdd.filter(log => { //执行位置在executor端
                !midsBC.value.contains(log.mid)
            })

            //关闭连接
            jedisClient.close()
            rddBC
        })
        value3

    }

    /**
     * 将去重后的结果mid写入redis，方便下个批次的数据做去重
     *
     * @param startUpLogDStream
     */
    def saveMidToRedis(startUpLogDStream: DStream[StartUpLog]) = {
        startUpLogDStream.foreachRDD(rdd => {
            rdd.foreachPartition(partition => {
                //1.获取redis连接
                val jedisClient: Jedis = RedisUtil.getJedisClient
                //2.在foreach算子中将数据写入redis
                partition.foreach(log => {
                    //rediskey
                    val redisKey: String = "DAU:" + log.logDate

                    //根据key将数据保存至redis
                    jedisClient.sadd(redisKey, log.mid)
                })
                jedisClient.close()
            })
        })
    }

}
