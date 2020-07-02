package com.latticeengines.spark.util

import java.util
import java.util.concurrent.ConcurrentHashMap

import com.latticeengines.common.exposed.util.{CipherUtils, LocationUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, udf}

private[spark] object CountryCodeUtils {
  @volatile var map: util.Map[String, String] = _

  def convert(input: DataFrame, countryName: String, countryCodeName: String, url: String, user: String,
              passwd: String, key: String, salt: String)
  : DataFrame = {
    // load data into map once
    initMap(url, user, passwd, key, salt)
    val spark = SparkSession.builder().appName("reading country code").getOrCreate()
    val broadCastDictionary = spark.sparkContext.broadcast(map)
    val nameFunc:  String => String = name => {
      val cleanName = LocationUtils.getStandardCountry(name)
      val result = if (cleanName == null) null else broadCastDictionary.value.get(cleanName)
      result
    }
    val nameUDF = udf(nameFunc)
    input.withColumn(countryCodeName, nameUDF(col(countryName)))
  }


  def initMap(url: String, user: String, passwd: String, key: String, salt: String): Unit = {
    if (map == null) {
      this.synchronized {
        if (map == null) {
          map = new ConcurrentHashMap
          val spark = SparkSession.builder().appName("reading country code").getOrCreate()
          val prop = new java.util.Properties()
          prop.put("user", user)
          prop.put("password", CipherUtils.decrypt(passwd, key, salt))
          prop.put("driver", "com.mysql.jdbc.Driver")
          val df: DataFrame = spark.read.jdbc(url, "CountryCode", prop)
          df.select("CountryName", "ISOCountryCode2Char").collect().par.foreach(row => {
            val name: String = row.getAs("CountryName")
            val isoCountryCode2Char: String = row.getAs("ISOCountryCode2Char")
            map.put(name, isoCountryCode2Char)
          })

        }
      }
    }
  }

}
