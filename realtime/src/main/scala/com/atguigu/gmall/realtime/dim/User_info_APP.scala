package com.atguigu.gmall.realtime.dim

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.{ProvinceInfo, User_info}
import com.atguigu.gmall.realtime.utils.{KafkaStreamUtils, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

/*
@author zilong-pan
@creat 2020-10-28 12:56
@desc $
*/ object User_info_APP {
  def main(args: Array[String]): Unit = {
    //公共流程
    val conf: SparkConf = new SparkConf().setAppName("User_info_APP").setMaster("yarn")
    val ssc = new StreamingContext(conf,Seconds(5))
    val topic = "ods_user_info"
    val groupid = "User_info_APP"
    val offsetRanges: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupid)
    var inputDStream: InputDStream[ConsumerRecord[String, String]]=null;
    var ranges: Array[OffsetRange]=Array.empty
    if(offsetRanges!=null&&offsetRanges.size>0)
    {
      inputDStream= KafkaStreamUtils.getKafkaStream(topic,ssc,offsetRanges,groupid)
    }else
    {
      inputDStream = KafkaStreamUtils.getKafkaStream(topic,ssc,groupid)
    }
    //精准一次性消费维护偏移量
    val offsetDstream: DStream[ConsumerRecord[String, String]] = inputDStream.transform {
      rdd => {
        ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    //================实际业务，保存到hbase=================================================
    offsetDstream.foreachRDD{
      rdd=>{

        val infordd: RDD[User_info] = rdd.map {
          data => {
            val str: String = data.value()
            val user_info: User_info = JSON.parseObject(str, classOf[User_info])
            val formattor = new SimpleDateFormat("yyyy-MM-dd")
            val date: Date = formattor.parse(user_info.birthday)
            val curTs: Long = System.currentTimeMillis()
            val  betweenMs = curTs - date.getTime
            val age = betweenMs/1000L/60L/60L/24L/365L
            if(age<20){
              user_info.age_group="20岁及以下"
            }else if(age>30){
              user_info.age_group="30岁以上"
            }else{
              user_info.age_group="21岁到30岁"
            }
            if(user_info.gender=="M"){
              user_info.gender_name="男"
            }else{
              user_info.gender_name="女"
            }
            user_info
          }
        }
        import org.apache.phoenix.spark._
        OffsetManagerUtil.saveOffset(topic,groupid,ranges)
        //id varchar primary key,info.name varchar,info.area_code varchar,info.iso_code varchar
        infordd.saveToPhoenix("GMALL0523_USER_INFO",Seq("ID", "USER_LEVEL", "BIRTHDAY", "GENDER","AGE_GROUP","GENDER_NAME"),new Configuration(),Some("hadoop102:2181"))

      }

    }
    ssc.start()
    ssc.awaitTermination()
  }

}
