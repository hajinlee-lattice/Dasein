package com.latticeengines.sparkdb.operator

import org.apache.avro.Schema.Field
import org.apache.avro.generic.GenericRecord
import org.apache.spark.rdd.RDD

abstract class DataOperator(val dataFlow: DataFlow) extends HasName with HasProperty {
  
  dataFlow.addOperator(this)
  
  def run(rdd: RDD[GenericRecord]): RDD[GenericRecord]
  
  def run(rdds: Array[RDD[GenericRecord]]): RDD[GenericRecord] = {
    run(rdds(0))
  }
  
  def getFields(record: GenericRecord): java.util.List[Field] = {
    record.getSchema().getFields()
  }
  
}