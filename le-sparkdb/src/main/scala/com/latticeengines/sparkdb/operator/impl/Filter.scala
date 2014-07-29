package com.latticeengines.sparkdb.operator.impl

import org.apache.avro.generic.GenericRecord
import org.apache.spark.rdd.RDD

import com.latticeengines.sparkdb.operator.DataFlow
import com.latticeengines.sparkdb.operator.DataOperator


class Filter(val df: DataFlow) extends DataOperator(df) {
  override def run(rdd: RDD[GenericRecord]): RDD[GenericRecord] = {
    val filterCondition = getPropertyValue(Filter.FilterCondition)
    rdd.filter(record => Filter.filterFunction(record, filterCondition)).asInstanceOf[RDD[GenericRecord]]
  }

}

object Filter {
  val FilterCondition = "FilterCondition"
    
  def filterFunction(record: GenericRecord, condition: String): Boolean = {
    true
  }
}