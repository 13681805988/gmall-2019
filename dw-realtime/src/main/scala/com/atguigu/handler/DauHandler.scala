package com.atguigu.handler

import com.atguigu.bean.StartUpLog
import com.atguigu.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {

  //1.跨批次去重 将不同批次的数据存入Redis
  def filterDataByRedis(startLog: DStream[StartUpLog]):DStream[StartUpLog] = {
    val filterByRedis: DStream[StartUpLog] = startLog.transform {
      rdd => {
        rdd.mapPartitions(
          iter => {
            val jedisClient: Jedis = RedisUtil.getJedisClient
            val filterLogs: Iterator[StartUpLog] = iter.filter {
              log => {
                val redisKey = s"dau${log.logDate}"
                !jedisClient.sismember(redisKey, log.mid)
              }
            }
            jedisClient.close()
            filterLogs
          }
        )
      }
    }
    filterByRedis
  }

//  在本批次中去重 先将数据转化为((date,mid),log) 再按照Key分组 最后flatMap取第一条数据
  def filterDataByBatch(filterByRedis: DStream[StartUpLog]):DStream[StartUpLog] = {
    val DateMidAndLog: DStream[((String, String), Iterable[StartUpLog])] = filterByRedis.map(
      log => {
        ((log.logDate, log.mid), log)
      }
    ).groupByKey()

    val filterDataDStream: DStream[StartUpLog] = DateMidAndLog.flatMap(
      log => {
        val logs: List[StartUpLog] = log._2.toList.sortWith(_.ts < _.ts).take(1)
        logs
      }
    )

    filterDataDStream
  }


















  //写入redis
  def saveMidToRedis(startLog: DStream[StartUpLog]): Unit ={
    startLog.foreachRDD{
      rdd=>{
        rdd.foreachPartition(
          iter=>{
            val jedisClient: Jedis = RedisUtil.getJedisClient
            iter.foreach(
              log=>{
                val redisKey: String = s"dau${log.logDate}"
                jedisClient.sadd(redisKey,log.mid)
              }
            )
            jedisClient.close()
          }
        )
      }
    }
  }

}
