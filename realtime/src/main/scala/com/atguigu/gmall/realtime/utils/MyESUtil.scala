package com.atguigu.gmall.realtime.utils

import java.util

import com.atguigu.gmall.realtime.bean.Start_log
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.Get.Builder
import io.searchbox.core.{Bulk, BulkResult, DocumentResult, Get, Index, Search, SearchResult}
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, QueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/*
@author zilong-pan
@creat 2020-10-21 13:00
*/
object MyESUtil {
  def bulk_insert(list: List[(Any,String)], index: String)={
      val jscli: JestClient = MyESUtil.getJscli()
       val builder = new Bulk.Builder
      for (elem <- list) {
        val indexop: Index = new Index.Builder(elem._1).index(index).
          `type`("_doc").id(elem._2).build()
          builder.addAction(indexop)

      }
      val bulk: Bulk = builder.build()
      val result: BulkResult = jscli.execute(bulk)
      jscli.close()
    //测试
//      val items: util.List[BulkResult#BulkResultItem] = result.getItems
//      println(items.size())

  }

  var cliFactory:JestClientFactory=null;
  def main(args: Array[String]): Unit = {
    search()

  }
  def build()
  {
    val url: String = PropertiesReaderUtils.loadProp("config.properties").getProperty("es.url")
    cliFactory=new JestClientFactory()
    cliFactory.setHttpClientConfig(
      new HttpClientConfig.Builder(url).
        multiThreaded(true).
        connTimeout(3000).maxTotalConnection(1000).readTimeout(2000).build())
  }
  def getJscli(): JestClient = {
    if (cliFactory == null) {
      build()
    }
    val jsclient: JestClient = cliFactory.getObject
    jsclient
  }

  //测试1：查询单条语句，观察是否通
  def getes(): Unit =
  {
    val result: DocumentResult = getJscli().execute(new Get.Builder("java_test","10").build())
    println(result.getJsonObject)
  }
  //测试添加数据：putindex
  def putindex(): Unit =
  {
    val str: String =
      """
        | {
        |    "name" : "zhangsan",
        |    "id":13
        |    }
        |""".stripMargin
    val client: JestClient = getJscli()
    val result: DocumentResult =client.execute(new Index.Builder(str).index("java_test").id("1").build())
    client.close()
  }
  //使用非es语法进行创建
  def putindex1(): Unit =
  {
    val client: JestClient = getJscli()
    client.execute(new Index.Builder(new student("wangwu",11)).index("java_test").`type`("test").id("11").build())
    client.close()
  }
  //使用es语法查看数据
  def search(): Unit =
  {

    val str: String =
      """
        |{
        |  "query": {
        |    "match": {
        |      "name": "wangwu"
        |    }
        |  }
        |}
        |""".stripMargin
    //第二种：对查询语句进行包装后转为string
    val builder = new SearchSourceBuilder()
    builder.query(new BoolQueryBuilder().must(new MatchQueryBuilder("name","lisi")))
    val str1: String = builder.toString
      val client: JestClient = getJscli()
      val result: SearchResult = client.execute(new Search.Builder(str1).addIndex("java_test").build())
      val list: util.List[SearchResult#Hit[student, Void]] = result.getHits(classOf[student])

    import scala.collection.JavaConverters._
    val list1: mutable.Buffer[SearchResult#Hit[student, Void]] = list.asScala
    val students: List[student] = list1.toList.map(_.source)
    println(students)
  }



}
case class student(name:String,id:Long)