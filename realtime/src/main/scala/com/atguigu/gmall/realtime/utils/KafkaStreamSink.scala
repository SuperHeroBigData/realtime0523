package com.atguigu.gmall.realtime.utils

import java.util
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.codehaus.jackson.map.deser.std.StringDeserializer
import org.elasticsearch.cluster.DiffableUtils.StringSetValueSerializer

/*
@author zilong-pan
@creat 2020-10-26 16:42
@desc kafka生产者配置
*/
object KafkaStreamSink {
  val properties: Properties = PropertiesReaderUtils.loadProp("config.properties")
  val str: String = properties.getProperty("kafka.broker.list")
  var kafkaProducer:KafkaProducer[String,String]=null;
  def createProducer(): KafkaProducer[String,String] =
  {
    if(kafkaProducer==null){
    val value = new Properties()
    value.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,str)
    value.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,(true:java.lang.Boolean))
    value.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer])
    value.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer])
    kafkaProducer=new KafkaProducer[String,String](value)
  }
    kafkaProducer
  }
  def send(op_topic: String, elem: String) = {
      createProducer().send(new ProducerRecord[String,String](op_topic,elem))
  }
  def send(op_topic: String, elem: String,key:String) = {
    createProducer().send(new ProducerRecord[String,String](op_topic,key,elem))
  }
//必须主动close
 /* def main(args: Array[String]): Unit = {

    send("spark_test","fskfjsfkjskfdsjakfjsakfjaskfjaskfjaksfjaskjfs")


  }*/

}
