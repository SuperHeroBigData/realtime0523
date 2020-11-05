package com.atguigu.gmall.realtime.utils

import java.util
import java.util.Properties

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/*
@author zilong-pan
@creat 2020-10-21 15:52
@desc redis连接创建工具类
*/
object RedisUtil {

    private var jedisPool: JedisPool = null
    def buildJedis(): Unit =
    {
        val properties: Properties = PropertiesReaderUtils.loadProp("config.properties")
        val host: String = properties.getProperty("redis.host")
        val port: String = properties.getProperty("redis.port")
        val config = new JedisPoolConfig()
        config.setMaxIdle(5)
        config.setMaxTotal(100)
        config.setMinIdle(5)
        config.setMaxWaitMillis(5000)
        config.setBlockWhenExhausted(true)
        config.setTestOnBorrow(true)
        jedisPool=new JedisPool(config,host)
    }
    def getRedis()=
    {
        if(jedisPool==null)
            {
                buildJedis()
            }
        val jedisCli: Jedis = jedisPool.getResource
        jedisCli
    }
//测试redis local模式，redis是否联通
    def main(args: Array[String]): Unit = {
        val jedis: Jedis = getRedis()
        println(jedis.ping())

        println(jedis.keys("*"))
    }
}
