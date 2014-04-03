package com.latticeengines.sparkdb

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object ABCount {
  def main(args: Array[String]) {
    val file = "/input/file1.txt"
    val conf = new SparkConf()
    val sc = new SparkContext("yarn-client", "Simple App", conf)
    val logData = sc.textFile(file, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}