package com.atguigu.gmall.realtime.dws

import java.util.Properties
import java.{lang, util}

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall.realtime.bean.{OrderDetail, OrderWide, Order_info}
import com.atguigu.gmall.realtime.utils.{BaseSparkStream, KafkaStreamSink, OffsetManagerUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/*
@author zilong-pan
@creat 2020-10-30 19:17
@desc $

 */

object OrderWiderAppbak {

  def main(args: Array[String]): Unit = {
    val order_info_topic:String="dwd_order_info"
    val order_info_groupid:String="dwd_order_info_group"
    val order_detail_topic:String="dwd_order_detail"
    val order_detail_groupid:String="dwd_order_detail_group"
    val ssc: StreamingContext = BaseSparkStream.createSsc("yarn","OrderWiderApp")
    val order_infotuple: (DStream[ConsumerRecord[String, String]], Array[OffsetRange]) = BaseSparkStream.get(ssc,order_info_topic,order_info_groupid)
    val order_detailtuple: (DStream[ConsumerRecord[String, String]], Array[OffsetRange]) = BaseSparkStream.get(ssc,order_detail_topic,order_detail_groupid)

    val orderInfoDStream: DStream[ConsumerRecord[String, String]] = order_infotuple._1
    val orderInfooffsetRanges: Array[OffsetRange] = order_infotuple._2
    val orderDetailDStream: DStream[ConsumerRecord[String, String]] = order_detailtuple._1
    val orderDetailoffsetRanges: Array[OffsetRange] = order_detailtuple._2
    //println(orderInfooffsetRanges)
    //TODO=======================业务流程===============================================
    //流格式转换-》（orderid，object）
    val orderInfoStream: DStream[(Long, Order_info)] = orderInfoDStream.map {
      record => {
        val jsonStr: String = record.value()
        val order_info: Order_info = JSON.parseObject(jsonStr, classOf[Order_info])
        (order_info.id, order_info)
      }
    }
    val orderDetailStream: DStream[(Long, OrderDetail)] = orderDetailDStream.map {
      record => {
        val jsonstr: String = record.value()
        val orderdetail: OrderDetail = JSON.parseObject(jsonstr, classOf[OrderDetail])
        (orderdetail.order_id, orderdetail)

      }
    }
    //双流join
    //TODO 方案一：单采集周期内fulljoin全外联orderinfo->A,order_detail->B
 /*   全外联三种情况
    1、(some,some)
      B后续可能有需要的orderinfo故将orderinfo保存到redis中，设置一定的过期时间
    2、（some,none）
      A有剩下的，需要取redis中取B中缓存的，而后做连接，且需要将其保存至redis中，为后面detail服务
    3、（none，some）
      B有剩下的，说明该周期内没有消费完，需要取redis中取A的数据，取不到需要将其保存至redis，后续关联
*/
    //方案一实现
    //主表保存采用String，key：orderinfo：orderid，value：orderinfojson
    //从表保存采用set ，key：orderdetail：orderid，value：orderdetailjson
    //orderInfoStream  orderDetailStream
   /* val fulljoin: DStream[(Long, (Option[Order_info], Option[OrderDetail]))] = orderInfoStream.fullOuterJoin(orderDetailStream,4)
    fulljoin.flatMap{
      case (orderid,(order_info_opt,order_detail_opt))=>{
        var orderwide:OrderWide=null;
        val jedis: Jedis = RedisUtil.getRedis()
        val list:ListBuffer[OrderWide]=ListBuffer.empty[OrderWide]
          if(order_info_opt!=None)
            {
              if(order_detail_opt!=None)
                {
                    orderwide=new OrderWide(order_info_opt.get,order_detail_opt.get)
                    list.append(orderwide)
                }
              //B为空，保存A，并读取B缓存
              val order_info: Order_info = order_info_opt.get
              val jsonstr: String = JSON.toJSONString(order_info,new SerializeConfig(true))
              val orderinfokey:String="orderinfo:"+order_info.id
              jedis.set(orderinfokey,jsonstr)
              jedis.expire(orderinfokey,100)
              //从jedis中取出B数据
              val orderdetailset: util.Set[String] = jedis.smembers("orderdetail:"+order_info.id)
              import scala.collection.JavaConverters._
              val detailList: List[OrderDetail] = orderdetailset.asScala.map{data=>{JSON.parseObject(data,classOf[OrderDetail])}}.toList
              for (elem <- detailList) {
                  list.append(new OrderWide(order_info,elem))
              }
            }else
          {
            //A为空，取A进行关联，而后写如redis
            val orderdetail: OrderDetail = order_detail_opt.get
            val orderinfoStr: String = jedis.get("orderinfo:"+orderdetail.order_id)
            list.append(new OrderWide(JSON.parseObject(orderinfoStr,classOf[Order_info]),orderdetail))
            //写B至缓存
            jedis.sadd("orderdetail:"+orderdetail.order_id,JSON.toJSONString(orderdetail,new SerializeConfig(true)))
            jedis.expire("orderdetail:"+orderdetail.order_id,100)
          }
      jedis.close()
        list
      }
    }*/

    //TODO 方案二：采用开窗，对累计周期内的数据进行汇总，但每次步长滑动会有重复数据，采用redis进行去重
    //开窗

    val orderdetailWindow: DStream[(Long, OrderDetail)] = orderDetailStream.window(Seconds(50),Seconds(5))
    val orderinfoWindow: DStream[(Long, Order_info)] = orderInfoStream.window(Seconds(50),Seconds(5))
    val joinStream: DStream[(Long, (OrderDetail, Order_info))] = orderdetailWindow.join(orderinfoWindow)
    val wideDstream: DStream[OrderWide] = joinStream.map {
      case (orderid, (orderdetail, orderinfo)) => {

       val wide = new OrderWide(orderinfo,orderdetail)
        wide
      }
    }
    val orderwideWindowRepeatStream: DStream[OrderWide] = wideDstream.mapPartitions {
      wideitr => {
        //思路：redis存储其采用set存储，order_join:[orderId]  value ? orderDetailId  expire : 60*10
        val list: List[OrderWide] = wideitr.toList
        val listbuffer: ListBuffer[OrderWide] = ListBuffer.empty[OrderWide];
        val jedis: Jedis = RedisUtil.getRedis()
        for (elem <- list) {
          val orderkey: String = "order_join:" + elem.order_id.toString
          val ordervalue = elem.order_detail_id.toString
          val isExitsValue: lang.Long = jedis.sadd(orderkey, ordervalue)
          jedis.expire(orderkey,100)
          if (isExitsValue == 1L) {
            listbuffer.append(elem)
          }
        }
        jedis.close()
        listbuffer.toIterator
      }
    }
  /*  //对去重后的数据，采用补全其分摊价格
    判断是否最后detail：当前商品总价=总原价格-其余商品价格汇总
    均摊方式：1、非最后商品：
                  当前商品价格*数量/订单总原价*订单实付款金额
             2、最后商品：
                  订单实付款金额-其他商品价格分摊汇总
    //redis 存储格式 orderid：split -》value
      //              orderid:ori
*/
    joinStream.join()
    val splitwindowDstream: DStream[OrderWide] = orderwideWindowRepeatStream.mapPartitions {
      wideitr => {
        val list: List[OrderWide] = wideitr.toList
        val listbuffer: ListBuffer[OrderWide] = ListBuffer.empty[OrderWide]
        val jedis: Jedis = RedisUtil.getRedis()
        for (elem <- list) {
          val orikey = elem.order_id.toString + ":ori"
          val splitkey = elem.order_id.toString + ":split"
          var oriamount: Double = 0L
          var splitamount: Double = 0L
          val oritotal: String = jedis.get(orikey)
//          println(oritotal)
          val splittotal: String = jedis.get(splitkey)
          //println(splittotal)
          val skutotal: Double = elem.sku_num * elem.sku_price.toDouble
          //待填充字段
          //分摊金额
          //var final_detail_amount: Double = 0D
          if (oritotal != null && (!oritotal.isEmpty)) {
            oriamount = oritotal.toDouble
          }
          if (splittotal != null && (!splittotal.isEmpty)) {
            splitamount = splittotal.toDouble
          }
          if (skutotal == elem.original_total_amount - oriamount) {
            //最后一个商品
            elem.final_detail_amount = Math.round((elem.final_total_amount - splitamount)*100)/100
          } else {
            //不是最后一个商品
            elem.final_detail_amount = Math.round(elem.final_total_amount * (skutotal / elem.original_total_amount) * 100D) / 100D
            //更新redis的数据
            jedis.set(orikey, (skutotal + oriamount).toString)
            jedis.expire(orikey,100)
            jedis.set(splitkey, (splitamount + elem.final_detail_amount).toString)
            jedis.expire(splitkey,100)
          }
          listbuffer.append(elem)

        }
        jedis.close()
        listbuffer.toIterator
      }
    }
    //splitwindowDstream.print(1000)
    //保存到clickhouse中
    val session: SparkSession = SparkSession.builder().appName("OrderWiderApp").getOrCreate()
    import session.implicits._
    splitwindowDstream.foreachRDD{
      rdd=>{
          rdd.cache()

          rdd.foreach{
            data=>{
              //println("执行到了么")
              KafkaStreamSink.send("dws_order_wide",JSON.toJSONString(data,new SerializeConfig(true)))
            }
          }

          val df: DataFrame = rdd.toDF()
          df.write.mode(SaveMode.Append).option("bachsize","100").option("isolationLevel","NONE").option("numPartitions",4).option("driver","ru.yandex.clickhouse.ClickHouseDriver").jdbc("jdbc:clickhouse://hadoop102:8123/default","t_order_wide_0523",new Properties())



          OffsetManagerUtil.saveOffset(order_info_topic,order_info_groupid,orderInfooffsetRanges)
          OffsetManagerUtil.saveOffset(order_detail_topic,order_detail_groupid,orderDetailoffsetRanges)

      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}

