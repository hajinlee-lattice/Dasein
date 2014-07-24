package com.latticeengines.sparkdb.operator

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import com.latticeengines.domain.exposed.dataplatform.HasName

trait DataOperator extends HasName with Logging {

  def run[T](rdd: RDD[T])
}