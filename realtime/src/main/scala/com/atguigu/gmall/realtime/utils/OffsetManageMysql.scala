package com.atguigu.gmall.realtime.utils

import java.util

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import scala.collection.mutable

/*
@author zilong-pan
@creat 2020-11-02 17:56
@desc $  
*/
object OffsetManageMysql {
  //测试
/*  def main(args: Array[String]): Unit = {
    val partitionToLong: Map[TopicPartition, Long] = getOffset("second","first")
    println(partitionToLong)

  }*/
  def getOffset(topic:String,groupid:String ): Map[TopicPartition,Long] =
  {
    val sql:String=s"select partition_id ,topic_offset from offset_0523 where group_id ='${groupid}' and topic='${topic}'"
    val jsonList: List[JSONObject] = MySQLUtil.query(sql)
    val offSetMAP: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition,Long]()
    for (elem <- jsonList) {
      val partitionid: Integer = elem.getInteger("partition_id")
      val topic_offset: Long = elem.getLongValue("topic_offset")
      println(elem)
      offSetMAP.put(new TopicPartition(topic,partitionid),topic_offset)
    }
    offSetMAP.toMap
  }
  /*
  @source：Array[OffsetRange]=》des: key=offset:topic:groupid value=hashmap(partition,offset)

   */
/*  def saveOffset(topic: String, groupId: String, ranges: Array[OffsetRange]) = {
    val offsetMap = new util.HashMap[String,String]()
    for (elem <- ranges) {
      val beginOffset: Long = elem.fromOffset
      val partition: Int = elem.partition
      val endOffset: Long = elem.untilOffset
      offsetMap.put(partition.toString,endOffset.toString)}
    //拼接redis中的key
    val key:String="offset:"+topic+":"+groupId
    if(offsetMap!=null&&offsetMap.size()!=0)
    {
      val jedisclient: Jedis = RedisUtil.getRedis()
      jedisclient.hmset(key,offsetMap)
      jedisclient.close()
    }*/
  //}
}
