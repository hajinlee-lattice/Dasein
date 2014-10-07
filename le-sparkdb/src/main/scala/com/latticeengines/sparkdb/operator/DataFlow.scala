package com.latticeengines.sparkdb.operator

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import scala.collection.mutable.MutableList

class DataFlow(private var name: String, private var conf: Configuration, private var local: Boolean = true) extends HasName {

  val sourceOperators = MutableList[DataOperator]()
  val operators = MutableList[DataOperator]()
  val job = new Job(conf)
  val sparkConf = new SparkConf().setAppName(name)
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  sparkConf.set("spark.kryo.registrator", "com.latticeengines.sparkdb.service.impl.LedpKryoRegistrator")

  if (local) {
    sparkConf.setMaster("local[4]")
  }
  val sc = new SparkContext(sparkConf)

  def addOperator(operator: DataOperator) = {
    operators += operator
  }
  
  def addSourceOperator(operator: DataOperator) = {
    sourceOperators += operator
  }

}