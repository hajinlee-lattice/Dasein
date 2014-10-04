package com.latticeengines.sparkdb.operator.impl

import org.apache.avro.generic.GenericRecord
import org.apache.spark.rdd.RDD

import com.latticeengines.sparkdb.operator.DataFlow
import com.latticeengines.sparkdb.operator.DataOperator

class Matcher(val df: DataFlow) extends DataOperator(df) {
  override def run(rdd: RDD[GenericRecord]): RDD[GenericRecord] = {
    null
  }

  override def getPropertyNames(): Set[String] = {
    return Set()
  }

}
