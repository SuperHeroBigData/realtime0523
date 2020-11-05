package com.atguigu.gmall.realtime.utils
import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.Properties

import com.twitter.chill.Base64.InputStream

/*
@author zilong-pan
@creat 2020-10-21 15:51
@get properties Tools
*/
object PropertiesReaderUtils {

  def loadProp(pathname: String) =
  {
    val prop = new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(pathname),StandardCharsets.UTF_8));
    prop
  }
//测试
  def main(args: Array[String]): Unit = {
    println(loadProp("config.properties").getProperty("kafka.broker.list"))
  }

}
