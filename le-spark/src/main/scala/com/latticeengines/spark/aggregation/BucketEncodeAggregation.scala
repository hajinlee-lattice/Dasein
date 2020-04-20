package com.latticeengines.spark.aggregation

import com.latticeengines.domain.exposed.datacloud.dataflow.BucketAlgorithm
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}

private[spark] class BucketEncodeAggregation(fields: Seq[StructField], algos: Seq[BucketAlgorithm]) extends UserDefinedAggregateFunction {

  override def inputSchema: org.apache.spark.sql.types.StructType = StructType(fields)

  override def bufferSchema: StructType = StructType(List(
    StructField("encoded", LongType)
  ))

  override def dataType: DataType = LongType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    input.toSeq.zipWithIndex.foreach(t => {
      val (obj, idx) = t
      val prev = buffer.getLong(0)
    })
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {}

  override def evaluate(buffer: Row): Any = buffer(0)

}
