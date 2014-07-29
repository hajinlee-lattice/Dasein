package com.latticeengines.sparkdb.operator

import scala.collection.JavaConverters._
import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema.Field
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

abstract class DataOperator(val dataFlow: DataFlow) extends HasName with HasProperty {
  
  dataFlow.addOperator(this)
  
  def run(rdd: RDD[(Int, GenericRecord)]): RDD[(Int, GenericRecord)]
  
  def run(rdds: Array[RDD[(Int, GenericRecord)]]): RDD[(Int, GenericRecord)] = {
    run(rdds(0))
  }
  
  def getFields(record: GenericRecord): java.util.List[Field] = {
    record.getSchema().getFields()
  }
  
}