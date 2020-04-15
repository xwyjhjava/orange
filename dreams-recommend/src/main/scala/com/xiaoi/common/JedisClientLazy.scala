package com.xiaoi.common

import java.util.Properties

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * JedisClientLazy
  * 通过使用lazy来实现redis的pool化
  *
  * Created by ligz on 16/2/22.
  */
object JedisClientLazy extends Serializable {

  val prop = new Properties()

  prop.load(this.getClass.getResourceAsStream("/redis.properties"))

  private val poolConfig = new GenericObjectPoolConfig()

  poolConfig.setMaxTotal(prop.getProperty("maxConn").toInt)

  poolConfig.setMaxIdle(prop.getProperty("maxConn").toInt)

  //  poolConfig.setMaxWaitMillis(prop.getProperty("maxWait").toLong)

  poolConfig.setMinIdle(0)
  //
  //  poolConfig.setTimeBetweenEvictionRunsMillis(prop.getProperty("evictRunInterval", "120000").toLong)
  //
  //  poolConfig.setMinEvictableIdleTimeMillis(prop.getProperty("evictTimeMills", "300000").toLong)

  //  lazy val pool = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout)

  lazy val pool = new JedisPool(poolConfig, prop.getProperty("host"),
    prop.getProperty("port").toInt, prop.getProperty("timeout").toInt, prop.getProperty("password"))

  val dbNums = prop.getProperty("dbNum", "1").toInt
  val pools = (0 to dbNums).map(i => {
    i match {
      case 0 => (0, pool)
      case _ => (i, new JedisPool(poolConfig, prop.getProperty("host"),
        prop.getProperty("port").toInt, prop.getProperty("timeout").toInt, prop.getProperty("password"), i))
    }
  })

  sys.addShutdownHook{
    println("Execute hook thread:" + this)
    pools.foreach(poolPair => poolPair._2.destroy())
  }

  val ONEHOUR = 60 * 60

  val ONEDAY = 24 * ONEHOUR

  val ONEWEEK = 7 * ONEDAY

  //  /**
  //   * 保存（key，value）信息到redis中
  //   * @param key
  //   * @param value
  //   */
  //  def save(key: String, value: String, expSecs: Int = ONEDAY): Unit = {
  //    val jedis = pool.getResource
  //    jedis.setex(key, expSecs, value)
  //    jedis.close()
  //  }

}
