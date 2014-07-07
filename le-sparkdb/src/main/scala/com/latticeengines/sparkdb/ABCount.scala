package com.latticeengines.sparkdb

import java.lang.Throwable
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object ABCount {
  def main(args: Array[String]) {
    val file = "/tmp/a.txt"
    val conf = new SparkConf().setAppName("App1")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(file, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    new Throwable().printStackTrace()
    
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}