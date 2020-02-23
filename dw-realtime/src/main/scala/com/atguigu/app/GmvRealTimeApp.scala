package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._
object GmvRealTimeApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("GmvRealTimeApp").setMaster("local[*]")
    val ssc = new  StreamingContext(sparkConf,Seconds(3))
    val kafkaDstream: InputDStream[(String, String)] =
      MyKafkaUtil.getKafkaStream(ssc,Set(GmallConstants.GMALL_ORDER_INFO_TOPIC))
    val orderInfoDstream: DStream[OrderInfo] = kafkaDstream.map {
      case (key, value) => {
        val orderinfo: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])
        //处理时间 将时间分开
        val dateAndHourTime: Array[String] = orderinfo.create_time.split(" ")
        orderinfo.create_date = dateAndHourTime(0)
        orderinfo.create_hour = dateAndHourTime(1).split(":")(0)
        //手机号码进行脱敏处理
        orderinfo.consignee_tel = orderinfo.consignee_tel.splitAt(5)._1 + "******"
        orderinfo
      }
    }

    orderInfoDstream.foreachRDD(
      orderInfo=>{
        orderInfo.saveToPhoenix(
          "GMALL190826_ORDER_INFO",
          Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
          new Configuration(), Some("hadoop102,hadoop103,hadoop104:2181")
        )
      }
    )
  ssc.start()
    ssc.awaitTermination()
  }
}
