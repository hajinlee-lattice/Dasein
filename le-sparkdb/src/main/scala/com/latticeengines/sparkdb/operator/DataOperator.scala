package com.latticeengines.sparkdb.operator

import scala.collection.JavaConverters._
import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema.Field
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait DataOperator extends HasName with HasProperty {

  def run(rdd: RDD[GenericRecord], job: Job, sc: SparkContext): RDD[GenericRecord]
  
  def getFields(rdd: RDD[GenericRecord]): java.util.List[Field] = {
    rdd.first().getSchema().getFields()
  }
}