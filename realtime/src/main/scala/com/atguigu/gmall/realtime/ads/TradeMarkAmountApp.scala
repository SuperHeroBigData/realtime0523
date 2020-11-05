package com.atguigu.gmall.realtime.ads

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.OrderWide
import com.atguigu.gmall.realtime.utils.{KafkaStreamUtils, MySQLUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

import scala.collection.mutable.ListBuffer

/*
@author zilong-pan
@creat 2020-11-03 13:10
@desc $  
*/ object TradeMarkAmountApp {
  def main(args: Array[String]): Unit = {
    val sparkconf: SparkConf = new SparkConf().setAppName("TradeMarkAmountApp").setMaster("yarn")
    val ssc = new StreamingContext(sparkconf,Seconds(5))
    val topic = "dws_order_wide"
    val groupid = "dws_order_wide"

    val offset: Map[TopicPartition, Long] = MySQLUtil.getMysqlOffset(topic,groupid)
    var inputDStream: InputDStream[ConsumerRecord[String, String]]=null
    if(OffsetManagerUtil.getOffset(topic,groupid)!=null&&OffsetManagerUtil.getOffset(topic,groupid).size!=0)
    {
      inputDStream= KafkaStreamUtils.getKafkaStream(topic,ssc,offset,groupid)
    }else
    {
      inputDStream = KafkaStreamUtils.getKafkaStream(topic,ssc,groupid)
    }
    var offranges: Array[OffsetRange]=null;
    val offsetDStream: DStream[ConsumerRecord[String, String]] = inputDStream.transform {
      rdd => {
        offranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    val withIdTmDStream: DStream[(String, Double)] = offsetDStream.map {
      orderwideRecorde => {
        val str: String = orderwideRecorde
          .value()
        val wide: OrderWide = JSON
          .parseObject(str, classOf[OrderWide])
        (wide.tm_id + "_" + wide.tm_name, wide.final_detail_amount)
      }
    }
    val value: DStream[(String, Double)] = withIdTmDStream.reduceByKey(_+_)
    //方式一：每条数据写入
    //方式二：成批写入
    value.foreachRDD{
      rdd=>{
        val DriverRdd: Array[(String, Double)] = rdd.collect()
        //println(DriverRdd)
        if(DriverRdd.length>0)
        {
          DBs.setup()
          DB.localTx{
            implicit session =>{
              val date_time: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
              val listSeq = new ListBuffer[Seq[Any]]()
              for (elem <- DriverRdd) {
                val totalAmount: Double = elem._2
                val id_name: Array[String] = elem._1.split("_")
                val id: String = id_name(0)
                val tm_name: String = id_name(1)
                val seq: Seq[Any] = Seq(date_time,id,tm_name,totalAmount)
                listSeq.append(seq)
              }
              SQL("insert into trademark_amount_stat values(?,?,?,?)").batch(listSeq.toSeq:_*).apply()
              for (elem <- offranges) {
                SQL("replace into offset_0523 values(?,?,?,?)").bind(groupid,topic,elem.partition,elem.untilOffset).update().apply()
              }
            }
          }
        }


      }
    }
    ssc.start()
    ssc.awaitTermination()

  }
}
