package com.latticeengines.sparkdb.operator.impl

import org.apache.avro.generic.GenericRecord
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

import com.latticeengines.common.exposed.util.AvroUtils
import com.latticeengines.sparkdb.operator.DataFlow
import com.latticeengines.sparkdb.operator.DataOperator

class Join(val df: DataFlow) extends DataOperator(df) {

  override def run(rdd: RDD[GenericRecord]): RDD[GenericRecord] = {
    null
  }

  override def run(rdds: Array[RDD[GenericRecord]]): RDD[GenericRecord] = {
    val joinCondition = getPropertyValue(Join.JoinCondition).asInstanceOf[String]

    val rdd1 = rdds(0).map {
      p => (p.get(Join.parseJoinCondition(joinCondition)(0)).asInstanceOf[String], p)
    }
    
    val rdd2 = rdds(1).map {
      p => (p.get(Join.parseJoinCondition(joinCondition)(1)).asInstanceOf[String], p)
    }
    
    val rdd1Broadcast = dataFlow.sc.broadcast(rdd1.collectAsMap())
    val schemaAndMap = AvroUtils.combineSchemas(rdds(0).first().getSchema(), rdds(1).first().getSchema())
    schemaAndMap(0) = schemaAndMap(0).toString()
    val joined = rdd2.mapPartitions({ iter =>
      val m = rdd1Broadcast.value
      for {
        (t, u) <- iter if m.contains(t)
      } yield (Join.combineRecords(u, m.get(t).get, schemaAndMap))
    }, preservesPartitioning = true)
    joined
  }

  override def getPropertyNames(): Set[String] = {
    return Set(Join.JoinCondition)
  }

}

object Join {

  val JoinCondition = "JoinCondition"

  def parseJoinCondition(expr: String) = {
    List("ConvertedOpportunityId", "Id")
  }

  def combineRecords(u: GenericRecord, v: GenericRecord, s: Array[java.lang.Object]): GenericRecord = {
    val combinedRecord = AvroUtils.combineAvroRecords(u, v, s)
    println(combinedRecord)
    combinedRecord
  }
}
