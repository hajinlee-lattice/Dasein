package com.latticeengines.sparkdb.operator.impl

import com.latticeengines.sparkdb.operator.{DataFlow, DataOperator}
import org.apache.avro.generic.GenericRecord
import org.apache.spark.rdd.RDD

class Transform(val df: DataFlow)  extends DataOperator(df) {

  override def run(rdd: RDD[GenericRecord]): RDD[GenericRecord] = {
    null
  }

}
