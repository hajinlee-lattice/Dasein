package com.latticeengines.sparkdb.operator.impl

import org.apache.avro.generic.GenericRecord
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

import com.latticeengines.sparkdb.operator.DataFlow
import com.latticeengines.sparkdb.operator.DataOperator

class Join (val df: DataFlow) extends DataOperator(df) {
  
  override def run(rdd: RDD[(Int, GenericRecord)]): RDD[(Int, GenericRecord)] = {
    null
  }

  override def run(rdds: Array[RDD[(Int, GenericRecord)]]): RDD[(Int, GenericRecord)] = {
    val rdd1Broadcast = dataFlow.sc.broadcast(rdds(0).collectAsMap())
    val joined = rdds(1).mapPartitions({ iter =>
      val m = rdd1Broadcast.value
      for {
        (t, u) <- iter
        if m.contains(t)
      } yield (t, Join.createGenericRecord(u, m.get(t).get))
    }, preservesPartitioning = true)
    joined
  }

}

object Join {
  
  def createGenericRecord(u: GenericRecord, v: GenericRecord): GenericRecord = {
    u
  }
}
