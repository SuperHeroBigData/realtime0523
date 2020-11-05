package com.atguigu.gmall.realtime.dim

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.ProvinceInfo
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
@creat 2020-10-28 10:45
@desc $
*/
object Province_info_APP {
  def main(args: Array[String]): Unit = {
    //公共流程
    val conf: SparkConf = new SparkConf().setAppName("Province_info_APP").setMaster("yarn")
    val ssc = new StreamingContext(conf,Seconds(5))
    val topic = "ods_base_province"
    val groupid = "Province_info_APP"
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

        val infordd: RDD[ProvinceInfo] = rdd.map {
          data => {
            val str: String = data.value()
            val info: ProvinceInfo = JSON.parseObject(str, classOf[ProvinceInfo])
            info
          }
        }
        import org.apache.phoenix.spark._
        OffsetManagerUtil.saveOffset(topic,groupid,ranges)
        //id varchar primary key,info.name varchar,info.area_code varchar,info.iso_code varchar
        infordd.saveToPhoenix("GMALL0523_PROVINCE_INFO",Seq("ID","INFO.NAME","INFO.AREA_CODE","INFO.ISO_CODE"),new Configuration(),Some("hadoop102:2181"))

        }

      }
    ssc.start()
    ssc.awaitTermination()
    }



}
