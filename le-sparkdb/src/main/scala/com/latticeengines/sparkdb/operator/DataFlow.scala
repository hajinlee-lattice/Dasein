package com.latticeengines.sparkdb.operator

import java.util.ArrayList
import java.util.List

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.spark._


class DataFlow(private var name: String, private var conf: Configuration, private var local: Boolean = true) extends HasName {

  val operators = new ArrayList[DataOperator]()
  val job = new Job(conf)
  val sparkConf = new SparkConf().setAppName(name)
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  
  if (local) {
    sparkConf.setMaster("local[4]")
  }
  val sc = new SparkContext(sparkConf)
  
  def addOperator(operator: DataOperator) = {
    operators.add(operator)
  }

}