package com.latticeengines.sparkdb.operator.impl

import org.apache.avro.generic.GenericRecord

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

import com.latticeengines.common.exposed.util.AvroUtils
import com.latticeengines.sparkdb.operator.DataFlow
import com.latticeengines.sparkdb.operator.DataOperator

import com.latticeengines.sparkdb.conversion.Implicits._

class Join(val df: DataFlow) extends DataOperator(df) {

  override def run(rdd: RDD[GenericRecord]): RDD[GenericRecord] = {
    null
  }

  override def run(rdds: Array[RDD[GenericRecord]]): RDD[GenericRecord] = {
    val joinCondition = getPropertyValue(Join.JoinCondition)

    val rdd1 = rdds(0).map {
      p => (p.get(Join.parseJoinCondition(joinCondition)(0)), p)
    }
    
    val rdd2 = rdds(1).map {
      p => (p.get(Join.parseJoinCondition(joinCondition)(1)), p)
    }

    val schemaAndMap = AvroUtils.combineSchemas(rdds(0).first().getSchema(), rdds(1).first().getSchema())
    schemaAndMap(0) = schemaAndMap(0).toString()

    val joined = rdd1.join(rdd2).map(p => {
      Join.combineRecords(p._2._1, p._2._2, schemaAndMap)
    })
    
    joined.foreach(println(_))
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
    AvroUtils.combineAvroRecords(u, v, s)
  }
}
