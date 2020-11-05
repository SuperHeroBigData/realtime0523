package com.atguigu.gmall.realtime.ods

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.utils.{KafkaStreamSink, KafkaStreamUtils, OffsetManagerUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
@author zilong-pan
@creat 2020-10-26 19:20
@desc $
*/

object KafkaToODS_M {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("yarn").setAppName("KafkaToODS_M")
    val ssc = new StreamingContext(new SparkContext(conf),Seconds(5))
    val topic = "gmall0523_db_m"
    val groupid = "gmall0523_db_m"
    var inputDStream: InputDStream[ConsumerRecord[String, String]]=null
    if(OffsetManagerUtil.getOffset(topic,groupid)!=null&&OffsetManagerUtil.getOffset(topic,groupid).size!=0)
      {
        inputDStream= KafkaStreamUtils.getKafkaStream(topic,ssc,OffsetManagerUtil.getOffset(topic,groupid),groupid)
      }else{
      inputDStream=KafkaStreamUtils.getKafkaStream(topic,ssc,groupid);
    }
    //获取偏移量，并保存在redis
    var ranges: Array[OffsetRange]=Array.empty
    val offsetStream: DStream[ConsumerRecord[String, String]] = inputDStream.transform {
      rdd => {
        ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    val jsonStream: DStream[JSONObject] = offsetStream.map {
      data => {
        val value: String = data.value()
        val jsonObject: JSONObject = JSON.parseObject(value)
        jsonObject
      }
    }
    jsonStream.foreachRDD{
      rdd=>{
        rdd.foreach{
          data=>{
            val op_type: String = data.getString("type")

            val jsondata: JSONObject = data.getJSONObject("data")
            val tableName: String = data.getString("table")
            if(jsondata!=null && !jsondata.isEmpty){
              if(
                ("order_info".equals(tableName)&&"insert".equals(op_type))
                  || (tableName.equals("order_detail") && "insert".equals(op_type))
                  ||  tableName.equals("base_province")
                  ||  tableName.equals("user_info")
                  ||  tableName.equals("sku_info")
                  ||  tableName.equals("base_trademark")
                  ||  tableName.equals("base_category3")
                  ||  tableName.equals("spu_info")

              ){
                var sendTopic :String = "ods_" + tableName
                //5.4 发送消息到kafka
                KafkaStreamSink.send(sendTopic,jsondata.toString)
              }
            }
          }
        }
        OffsetManagerUtil.saveOffset(topic,groupid,ranges)
      }
    }
    ssc.start()
    ssc.awaitTermination()

  }
}
