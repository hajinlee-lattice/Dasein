package com.latticeengines.sparkdb.operator.impl

import org.apache.avro.generic.GenericRecord
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

import com.latticeengines.sparkdb.operator.DataFlow
import com.latticeengines.sparkdb.operator.DataOperator

class Join (val df: DataFlow) extends DataOperator(df) {
  
  override def run(rdd: RDD[GenericRecord]): RDD[GenericRecord] = {
    null
  }

  override def run(rdds: Array[RDD[GenericRecord]]): RDD[GenericRecord] = {
    val joinCondition = getPropertyValue(Join.JoinCondition)
    
    val rdd1 = rdds(0).map(
        p => (p.get(Join.parseJoinCondition(joinCondition)(0)).asInstanceOf[Int], p))
    val rdd2 = rdds(1).map(
        p => (p.get(Join.parseJoinCondition(joinCondition)(1)).asInstanceOf[Int], p))
    val rdd1Broadcast = dataFlow.sc.broadcast(rdd1.collectAsMap())
    val joined = rdd2.mapPartitions({ iter =>
      val m = rdd1Broadcast.value
      for {
        (t, u) <- iter if m.contains(t)
      } yield (Join.createGenericRecord(u, m.get(t).get))
    }, preservesPartitioning = true)
    joined
  }

}

object Join {
  
  val JoinCondition = "JoinCondition"
    
  def parseJoinCondition(expr: String) = {
    List("Nutanix_EventTable_Clean", "Nutanix_EventTable_Clean")
  }
  
  def createGenericRecord(u: GenericRecord, v: GenericRecord): GenericRecord = {
    u
  }
}
