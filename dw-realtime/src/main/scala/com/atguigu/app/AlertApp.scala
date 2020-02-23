package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstants
import com.atguigu.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks._


object AlertApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(3))
    val kafkaDstream: InputDStream[(String, String)] = MyKafkaUtil
      .getKafkaStream(ssc,Set(GmallConstants.KAFKA_TOPIC_EVENT))
    val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val eventLogDStream: DStream[EventLog] = kafkaDstream.map {
      case (key, value) => {
        val eventLog: EventLog = JSON.parseObject(value, classOf[EventLog])
        val ts: Long = eventLog.ts
        val dateAndHourMinutes: String = simpleDateFormat.format(new Date(ts))
        val dateAndHourMinutesArray: Array[String] = dateAndHourMinutes.split(" ")
        eventLog.logDate = dateAndHourMinutesArray(0)
        eventLog.logHour = dateAndHourMinutesArray(1)
        eventLog
      }
    }
    //同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，
    // 并且在登录到领劵过程中没有浏览商品。达到以上要求则产生一条预警日志
    //先满足同一设备 和 5分钟内的条件
    val windowEventLogDStream: DStream[EventLog] = eventLogDStream.window(Seconds(30))
    val midToLogIter: DStream[(String, Iterable[EventLog])] =
      windowEventLogDStream.map(log=>(log.mid,log)).groupByKey()
    //新建一个uids用于存放领取优惠券登录过的uid
    val couponAlertInfoDstream: DStream[CouponAlertInfo] = midToLogIter.map {
      case (mid, logIter) => {
        val uids: util.HashSet[String] = new util.HashSet[String]()
        val itemlds: util.HashSet[String] = new util.HashSet[String]()
        val events: util.ArrayList[String] = new util.ArrayList[String]()
        var noClickAction: Boolean = true
        breakable {
          logIter.foreach(
            log => {
              events.add(log.evid)
              if ("clickItem".equals(log.evid)) {
                noClickAction = false
                break()
              } else if ("coupon".equals(log.evid)) {
                uids.add(log.uid)
                itemlds.add(log.itemid)
              }
            }
          )
        }
        (uids.size() >= 3 && noClickAction,
          CouponAlertInfo(mid, uids, itemlds, events, System.currentTimeMillis()))
      }
    }.filter(_._1).map(_._2)

    couponAlertInfoDstream.print()

    ssc.start()
    ssc.awaitTermination()
    //mid  	设备id
//    uids	领取优惠券登录过的uid
//    itemIds	优惠券涉及的商品id
//    events  	发生过的行为
//    ts	发生预警的时间戳
  }
}
