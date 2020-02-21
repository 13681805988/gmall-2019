package com.atguigu.app
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.DauHandler
import com.atguigu.util.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._
import scala.util.parsing.json.JSONObject

object DauApp{
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(2))
    val kafkaDStream: InputDStream[(String, String)] =
      MyKafkaUtil.getKafkaStream(ssc,Set(GmallConstants.KAFKA_TOPIC_STARTUP))
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val startLog: DStream[StartUpLog] = kafkaDStream.map {
      case (key, value) => {
        val log: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])
        val ts: Long = log.ts
        val dateAndHour: String = sdf.format(new Date(ts))
        val dateAndHourArr: Array[String] = dateAndHour.split(" ")
        log.logDate = dateAndHourArr(0)
        log.logHour = dateAndHourArr(1)
        log
      }
    }
    //跨批次去重 先写入Redis
    val filterByRedis: DStream[StartUpLog] = DauHandler.filterDataByRedis(startLog)
    filterByRedis.cache()

    //同一批次中去重
    val filterByBatch: DStream[StartUpLog] = DauHandler.filterDataByBatch(filterByRedis)
    filterByBatch.cache()

    //写入redis
    DauHandler.saveMidToRedis(filterByBatch)

    filterByRedis.count().print()
    filterByBatch.count().print()

    filterByBatch.foreachRDD(rdd =>
      rdd.saveToPhoenix("GMALL190826_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE",
          "LOGHOUR", "TS"), new Configuration,
        Some("hadoop102,hadoop103,hadoop104:2181"))
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
