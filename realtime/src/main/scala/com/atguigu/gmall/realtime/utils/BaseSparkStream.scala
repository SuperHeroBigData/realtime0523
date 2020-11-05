package com.atguigu.gmall.realtime.utils

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

/*
@author zilong-pan
@creat 2020-10-30 19:18
@desc $  
*/
object BaseSparkStream {
 /* def main(args: Array[String]): Unit = {
    val sparkconf: SparkConf = new SparkConf().setAppName("KafkaToODS").setMaster("local[4]")
    val ssc = new StreamingContext(sparkconf,Seconds(5))
    val topic = "ods_coupon_use"
    val groupid = "ods_coupon_use_000"
   val dsTest: DStream[ConsumerRecord[String, String]] = get(ssc,topic,groupid)

    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange] //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = dsTest.transform {
      rdd => {
        //周期性在driver中执行
        offsetRanges= rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    println(offsetRanges.size)
    inputGetOffsetDstream.print()

//    tuple._1.map(_.value()).print(1000)
    ssc.start()
    ssc.awaitTermination()
  }*/
    def get(ssc:StreamingContext,topic:String,groupId: String): Tuple2[DStream[ConsumerRecord[String, String]],Array[OffsetRange]] ={

      //从redis中读取偏移量
      val offsetMapForKafka: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)

      //通过偏移量到Kafka中获取数据
      var recordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
      if (offsetMapForKafka != null && offsetMapForKafka.size > 0) {
        recordInputDstream = KafkaStreamUtils.getKafkaStream(topic, ssc, offsetMapForKafka, groupId)
      } else {
        recordInputDstream = KafkaStreamUtils.getKafkaStream(topic, ssc, groupId)
      }


      //从流中获得本批次的 偏移量结束点（每批次执行一次）
      var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange] //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
      val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputDstream.transform {
        rdd => {
          //周期性在driver中执行
          offsetRanges= rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd
        }
      }

      //println(offsetRanges(0))
      (inputGetOffsetDstream,offsetRanges)
    }
    def createSsc(master:String,appname:String): StreamingContext ={
      val sparkConf: SparkConf = new SparkConf().setMaster(master).setAppName(appname)
      val ssc = new StreamingContext(sparkConf, Seconds(5))
      ssc
    }

  /*def get(ssc:StreamingContext,topic:String,groupId: String): DStream[ConsumerRecord[String, String]] ={

    //从redis中读取偏移量
    val offsetMapForKafka: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)

    //通过偏移量到Kafka中获取数据
    var recordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMapForKafka != null && offsetMapForKafka.size > 0) {
      recordInputDstream = KafkaStreamUtils.getKafkaStream(topic, ssc, offsetMapForKafka, groupId)
    } else {
      recordInputDstream = KafkaStreamUtils.getKafkaStream(topic, ssc, groupId)
    }

    recordInputDstream
//    //从流中获得本批次的 偏移量结束点（每批次执行一次）
//    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange] //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
//    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputDstream.transform {
//      rdd => {
//        //周期性在driver中执行
//        offsetRanges= rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//        rdd
//      }
//    }
//
//    //println(offsetRanges(0))
//    (inputGetOffsetDstream,offsetRanges)
  }*/
}
