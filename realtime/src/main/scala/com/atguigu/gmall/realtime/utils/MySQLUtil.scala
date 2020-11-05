package com.atguigu.gmall.realtime.utils

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/*
@author zilong-pan
@creat 2020-11-02 16:35
@desc $  
*/
object MySQLUtil {
  /*def main(args: Array[String]): Unit = {
    /*//测试连接是否生效
    println(query("select * from offset_0523"))*/
    //测试获取偏移量

    val partitionToLong: Map[TopicPartition, Long] = getMysqlOffset("dws_order_wide","dws_order_wide")
    for (elem <- partitionToLong) {
      println(elem._2)
    }
  }*/


  def query(sql:String): List[JSONObject] =
  {
      //反射注册驱动
    Class.forName("com.mysql.jdbc.Driver")
    //获取连接
    val con: Connection = DriverManager
      .getConnection("jdbc:mysql://hadoop102:3306/rt_mysql_ads?characterEncoding=utf-8&useSSL=false","root","123456")
    //预编译sql
    val statement: PreparedStatement = con.prepareStatement(sql)
    //执行sql
    val result: ResultSet = statement.executeQuery()
    //处理数据
    val jsonList = new ListBuffer[JSONObject]()
    val data: ResultSetMetaData = result.getMetaData
    while(result.next())
      {
        val json=new JSONObject()
        println(data.getColumnCount)
        for(i <-1 to data.getColumnCount)
          {
                json.put(data.getColumnName(i),result.getObject(i))

          }
        jsonList.append(json)
      }
    //关闭连接
    result.close()
    statement.close()
    con.close()
    jsonList.toList
  }
  def getMysqlOffset(topic:String,groupid:String):Map[TopicPartition, Long]=
  {
    //拼接sql
    val sql=s"select * from offset_0523 where topic='${topic}' and group_id='${groupid}'"
    val objects: List[JSONObject] = query(sql)
   val partitionToLong: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition,Long]()

    for (elem <- objects) {
      partitionToLong.put(new TopicPartition(elem.getString(topic),elem.getString("partition_id").toInt),elem.getString("topic_offset").toLong)
    }
    partitionToLong.toMap
  }

}
