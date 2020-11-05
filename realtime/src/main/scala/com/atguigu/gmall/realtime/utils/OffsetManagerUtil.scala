package com.atguigu.gmall.realtime.utils

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import scala.collection.mutable

/*
@author zilong-pan
@creat 2020-10-23 16:47
@desc $
*/
object OffsetManagerUtil {
  /*
  redis存储数据 hash
  key=offset:topic:groupid
  value=hashmap(partition,offset)
   */
  def getOffset(topic:String,groupid:String ): Map[TopicPartition,Long] =
  {
        val jedisclient: Jedis = RedisUtil.getRedis()
        //拼接key
        val key: String = "offset"+":"+topic+":"+groupid
        val offset: util.Map[String, String] = jedisclient.hgetAll(key)
        jedisclient.close()
        import scala.collection.JavaConverters._
        val offmap: Map[String, String] = offset.asScala.toMap
        val map: Map[TopicPartition, Long] = offmap.map {
           case (partition, offset) => {
          (new TopicPartition(topic, partition.toInt), offset.toLong)
        }
         }.toMap
        map
  }
  /*
  @source：Array[OffsetRange]=》des: key=offset:topic:groupid value=hashmap(partition,offset)

   */
  def saveOffset(topic: String, groupId: String, ranges: Array[OffsetRange]) = {
    val offsetMap = new util.HashMap[String,String]()
    for (elem <- ranges) {
      val beginOffset: Long = elem.fromOffset
      val partition: Int = elem.partition
      val endOffset: Long = elem.untilOffset
      offsetMap.put(partition.toString,endOffset.toString)
      println("partition: " + partition + ",beginOffset: " + beginOffset + ",endOffset: " + endOffset)
    }
    //拼接redis中的key
    val key:String="offset:"+topic+":"+groupId
    if(offsetMap!=null&&offsetMap.size()!=0)
      {
        val jedisclient: Jedis = RedisUtil.getRedis()
        jedisclient.hmset(key,offsetMap)
        jedisclient.close()
      }
  }

}
