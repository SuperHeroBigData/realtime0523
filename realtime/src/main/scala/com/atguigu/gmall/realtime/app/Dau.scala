package com.atguigu.gmall.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.Start_log
import com.atguigu.gmall.realtime.utils.{KafkaStreamUtils, MyESUtil, RedisUtil, OffsetManagerUtil}
import io.searchbox.client.JestClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/*
@author zilong-pan
@creat 2020-10-21 19:44
@desc  sparkstreaming处理kafka日志数据，解析时间，日期添加到jsonobject，
*/
object Dau {
   private var sparkconf :SparkConf=null

  def SparkconfSet(appname:String="my",master:String="yarn"): SparkConf =
  {
      if(sparkconf==null)
        {
          sparkconf=new SparkConf();
        }
      sparkconf.setAppName(appname).setMaster(master)
  }

  def main(args: Array[String]): Unit = {
    val topic:String="gmall_start_bak";
    val groupId="dau"
    //四个分区，正好四个executor处理
    val ssc = new StreamingContext(SparkconfSet("kafka_deal","local[4]"),Seconds(3))
    val offsetmap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset("gmall_start_bak","dau")
    var dsDream: InputDStream[ConsumerRecord[String, String]]=null;
    if(offsetmap!=null&&offsetmap.size!=0)
      {
        dsDream= KafkaStreamUtils.getKafkaStream("gmall_start_bak", ssc, offsetmap, "dau")
      }else
      {
        dsDream=KafkaStreamUtils.getKafkaStream("gmall_start_bak",ssc,"dau")
      }
    //从dstream中取出消费偏移量
    var ranges: Array[OffsetRange] = Array.empty
    val offsetDstream: DStream[ConsumerRecord[String, String]] = dsDream.transform {
      rdd => {
        ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //println("0号分区的offset范围：" + ranges(0).untilOffset)
        rdd
      }
    }

    val modi: DStream[JSONObject] = offsetDstream.map {
      result => {
        val str: String = result.value()
        val json: JSONObject = JSON.parseObject(str)
        val time: Long = json.getLongValue("ts")
        val dt: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(time))
        val dtArr: Array[String] = dt.split(" ")
        val date_now: String = dtArr(0)
        val hour: String = dtArr(1)
        json.put("dt", date_now)
        json.put("hr", hour)
        json
      }
    }
    /*
    function：筛选数据，并将数据匹配redis，完成日登陆日志的过滤
    short：每条json数据都要与redis进行匹配操作，连接redis次数过多
     */
   /* modi.filter{
      json=>{
        val dau_dt: String = json.getString("dt")+"dau"
        val mid: String = json.getJSONObject("common").getString("mid")
        val jedis: Jedis = RedisUtil.getRedis()
        val isAlive: lang.Long = jedis.sadd(dau_dt,mid)
        val isSet: lang.Long = jedis.ttl(dau_dt)
        if(isSet<0)
          {
            jedis.expire(dau_dt,3600*24)
          }
        jedis.close();
        if(isAlive>0)
          {
            true
          }else
          {
            false
          }
      }
    }*/
    /*
    方式2：以分区为单位，一次处理一个分区的数据，可减少与redis连接次数
     */
    val distinct: DStream[JSONObject] = modi.mapPartitions {
      jsonitr => {
        val jedis: Jedis = RedisUtil.getRedis()
        val list = new ListBuffer[JSONObject]
        for (json <- jsonitr) {
          val dau_dt: String = json.getString("dt") + "dau"
          val mid: String = json.getJSONObject("common").getString("mid")
          val isAlive: lang.Long = jedis.sadd(dau_dt, mid)
          val isSet: lang.Long = jedis.ttl(dau_dt)
          if (isSet < 0) {
            jedis.expire(dau_dt, 3600 * 24)
          }
          if (isAlive > 0) {
            list.append(json)
          }
        }
        jedis.close()
        list.toIterator
      }
    }
    /*
      去重后，将数据存储到es中
     */
    distinct.foreachRDD{
      rdd=>{
        rdd.foreachPartition{
          json=>{
              //封装成start_log对象，保存

              val dauList: List[(Start_log,String)] = json.map {
                jsonObj => {
                  //每次处理的是一个json对象   将json对象封装为样例类
                  val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
                  (Start_log(
                    commonJsonObj.getString("mid"),
                    commonJsonObj.getString("uid"),
                    commonJsonObj.getString("ar"),
                    commonJsonObj.getString("ch"),
                    commonJsonObj.getString("vc"),
                    jsonObj.getString("dt"),
                    jsonObj.getString("hr"),
                    "00",
                    jsonObj.getLong("ts")
                  ),commonJsonObj.getString("mid"))
                }
              }.toList
              //对分区的数据进行批量处理
              //获取当前日志字符串
              val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
              MyESUtil.bulk_insert(dauList,"gmall2020_dau_info_" + dt)
          }
        }
        OffsetManagerUtil.saveOffset(topic,groupId,ranges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
