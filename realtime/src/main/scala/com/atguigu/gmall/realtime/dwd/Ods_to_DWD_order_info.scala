package com.atguigu.gmall.realtime.dwd

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.{Order_info, ProvinceInfo, User_Status, User_info}
import com.atguigu.gmall.realtime.utils.{HbaseUtils, KafkaStreamSink, KafkaStreamUtils, MyESUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.ConfigurationUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/*
@author zilong-pan
@creat 2020-10-27 18:37
@desc $
*/
object Ods_to_DWD_order_info {
  def main(args: Array[String]): Unit = {
    //公共流程
    val conf: SparkConf = new SparkConf().setAppName("Ods_to_DWD_order_info").setMaster("yarn")
    val ssc = new StreamingContext(conf,Seconds(5))
    val topic = "ods_order_info"
    val groupid = "Ods_to_DWD_order_info"
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
    //------------------------------业务处理
    //1、ods层分流后的数据对数据进行包装，转换为样例类格式，方便计算
    val order_infoDStream: DStream[Order_info] = offsetDstream.map {
      record => {
        val str: String = record.value()
        val order_info: Order_info = JSON.parseObject(str, classOf[Order_info])
        val create_date: String = order_info.create_time
        //时间转换
        val dateArr: Array[String] = create_date.split(" ")
        order_info.create_date = dateArr(0)
        order_info.create_hour = dateArr(1).split(":")(0)
        order_info
      }
    }
    //2、更新是否首次登陆状态
    //方案一：每条数据执行一次sql语句，查询到则进行修改，否则对其是否首次登陆进行置为1
    //缺点：每条数据，访问一次hbase，频繁访问hbase效率低
/*    val fisrt: DStream[Order_info] = order_infoDStream.map {
      order_info => {
        val sql = s"select user_id from user_status0523 where user_id='${order_info.user_id}'"
        val querylist: List[JSONObject] = HbaseUtils.query(sql)
        if (querylist.isEmpty) {
          order_info.if_first_order = "1"
        } else {
          order_info.if_first_order = "0"
        }
        order_info
      }
    }*/
    //方案二：采用以分区为单位，每个分区执行一次sql，避免连接数量过多
    val updateDStream: DStream[Order_info] = order_infoDStream.mapPartitions {
      partitioniter => {
        val list: List[Order_info] = partitioniter.toList
        val user_id_list: List[Long] = list.map(_.user_id)
        val hbasestr: String = user_id_list.mkString("','")
        // select *from user_status0523 where user_id in ('1','2');
        val sql: String = "select user_id from user_status0523 where user_id in ('" + s"${hbasestr}" + "')"
        val querylist: List[String] = HbaseUtils.query(sql).map(_.getString("USER_ID"))
        //println(querylist)
        val order_hbase: List[Order_info] = list.map {
          data => {
            if (querylist.contains(data.user_id.toString)) {
              data.if_first_order = "0"
            } else {
              data.if_first_order = "1"
            }
            data
          }
        }
        order_hbase.toIterator
      }
    }
    //3、对同一用户同一采集周期内，多次下单使其同一采集周期内有同一用户有多单标记为首单
    val groupbyDStream: DStream[(Long, Iterable[Order_info])] = updateDStream.map(data=>(data.user_id,data)).groupByKey()
    val sortDStream: DStream[Order_info] = groupbyDStream.flatMap {
      case (user, itr) => {
        val list: List[Order_info] = itr.toList
        if (list != null && list.size > 1) {
          //同一周期内存在相同用户同时下多单
          val order_infoes: List[Order_info] = list.sortWith {
            case (data1, data2) => {
              data1.create_time < data2.create_time
            }
          }
          if (order_infoes(0).if_first_order == "1") {
            for (i <- 1 to order_infoes.size-1) {
              order_infoes(i).if_first_order = "0"
            }
          }
          order_infoes
        } else {
          list
        }
      }
    }
    //4、关联地区维度及用户维度
    //方式一：单条数据，每条数据执行一次sql查询数据，并执行
    //方式二：采用以分区为单位进行保存
    // ID  | NAME  | AREA_CODE  | ISO_CODE
    /*val order_infowithPro: DStream[Order_info] = sortDStream.mapPartitions {
      orderlist => {
        val list: List[Order_info] = orderlist.toList
        val province_idlist: List[String] = list.map(_.province_id.toString)
        val sql: String = s"select id,name,area_code,iso_code from gmall0523_province_info where id in (' ${province_idlist.mkString("','")}')"
        val json: List[JSONObject] = HbaseUtils.query(sql)
        val order_tuple_list: List[(String, ProvinceInfo)] = json.map {
          jsonobj => {
            val pro: ProvinceInfo = JSON.toJavaObject(jsonobj, classOf[ProvinceInfo])
            (pro.id, pro)
          }
        }
        val order_tuple_map: Map[Long, Order_info] = order_tuple_list.toMap
        list.map {
          order_info => {
            if (order_tuple_map != null && (!order_tuple_map.isEmpty)) {
              val order_info1: Order_info = order_tuple_map.getOrElse(order_info.province_id, null)
              order_info.province_iso_code = order_info1.province_iso_code
              order_info.province_area_code = order_info1.province_area_code
              order_info.province_name = order_info1.province_name
            }
            order_info
          }
        }
        list.toIterator
      }
    }*/

    //方式三：采用以每个采集周期，并使用广播变量方式查询hbase数据
    val order_with_proc: DStream[Order_info] = sortDStream.transform {
      rdd => {
        val sql: String = "select id,name,area_code,iso_code from gmall0523_province_info"
        val provlist: List[JSONObject] = HbaseUtils.query(sql)
        val provmap: Map[String, ProvinceInfo] = provlist.map {
          json => {
            val info: ProvinceInfo = JSON.toJavaObject(json, classOf[ProvinceInfo])
            (info.id, info)
          }
        }.toMap
        val provbroadcast: Broadcast[Map[String, ProvinceInfo]] = ssc.sparkContext.broadcast(provmap)
        val newrdd: RDD[Order_info] = rdd.map {
          orderinfo => {
            val provcast: Map[String, ProvinceInfo] = provbroadcast.value
            if (provcast != null && (provcast.size > 0)) {
              val info1: ProvinceInfo = provcast.getOrElse(orderinfo.province_id.toString, null)
              orderinfo.province_area_code = info1.area_code
              orderinfo.province_iso_code = info1.iso_code
              orderinfo.province_name = info1.name
            }
            orderinfo
          }
        }
        newrdd
      }
    }
    //5、对用户维度按照分区进行关联
    val order_with_user: DStream[Order_info] = order_with_proc.mapPartitions {
      orderiter => {
        val list: List[Order_info] = orderiter.toList
        val useridlist: List[Long] = list.map(_.user_id)
        // ID  | USER_LEVEL  |  BIRTHDAY   | GENDER  | AGE_GROUP  | GENDER_NAME
        val sql: String = s"select id,age_group,gender_name from gmall0523_user_info where id in ('${useridlist.mkString("','")}')"
        val jsonquery: List[JSONObject] = HbaseUtils.query(sql)
        val userMap: Map[String, User_info] = jsonquery.map {
          json => {
            val user_info: User_info = JSON.toJavaObject(json, classOf[User_info])
            (user_info.id, user_info)
          }
        }.toMap
        val order_infoes: List[Order_info] = list.map {
          order => {
            if (userMap != null && userMap.size > 0) {
              val user_info: User_info = userMap.getOrElse(order.user_id.toString, null)
              order.user_age_group = user_info.age_group
              order.user_gender = user_info.gender_name
            }
            order
          }
        }
        order_infoes.toIterator
      }
    }

    //6、对hbase中的首单标记为1进行数据保存,并将处理后的结果保存到es中进行可视化展示
    order_with_user.foreachRDD {
      rdd => {
        rdd.cache()
        val fileterRdd: RDD[Order_info] = rdd.filter(_.if_first_order == "1")
        val mapRDD: RDD[User_Status] = fileterRdd.map(data => {
          //println(data)
          User_Status(data.user_id.toString(), "1")
        })
        import org.apache.phoenix.spark._
        mapRDD.saveToPhoenix("user_status0523", Seq("USER_ID", "IF_CONSUMED"), new Configuration(), Some("hadoop102,hadoop103,hadoop104:2181"))
        rdd.foreachPartition{
          orderiter=>{
            val list: List[Order_info] = orderiter.toList
            val list_tuples: List[(Order_info, String)] = list.map {
              order => {
                //println(order)
                KafkaStreamSink.send("dwd_order_info",JSON.toJSONString(order,new SerializeConfig(true)))
                (order, order.id.toString)
              }
            }
            val datestr: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            MyESUtil.bulk_insert(list_tuples,s"gmall0523_order_info_${datestr}")
          }
        }
        OffsetManagerUtil.saveOffset(topic,groupid,ranges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
