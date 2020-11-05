package com.atguigu.gmall.realtime.utils

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer

/*
@author zilong-pan
@creat 2020-10-27 17:54
@desc $
*/
object HbaseUtils {
  //测试hbase连接及方法是否可用
  def main(args: Array[String]): Unit = {

      val objects: List[JSONObject] = query("select *from user_status0523")
    println(objects)
  }
  def query(sql:String )={//返回查询结果{jsonobject，jsonobject}可以有多条
    var list: ListBuffer[JSONObject] = new ListBuffer[JSONObject]()
    //注册驱动
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    //获取连接
    val con: Connection = DriverManager.getConnection("jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181")
    //预编译sql
    val statement: PreparedStatement = con.prepareStatement(sql)
    //执行sql,返回结果
    val result: ResultSet = statement.executeQuery()
    val metadata: ResultSetMetaData = result.getMetaData
    //结果处理
    while(result.next())
    {
      //存放单条查询结果
      var json = new JSONObject()
      for(i<-1 to metadata.getColumnCount)
      {
        json.put(metadata.getColumnName(i),result.getObject(i))
      }
      list.append(json)
    }
    //关闭连接
    result.close()
    statement.close()
    con.close()
    list.toList
  }
}
