package com.latticeengines.spark.aggregation

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

private[spark] class ConcatStringsUDAF(InputColumnName: String, sep: String) extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = StructType(StructField(InputColumnName, StringType) :: Nil)

  override def bufferSchema: StructType = StructType(StructField("concatString", StringType) :: Nil)

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) = ""

  private def concatStrings(str1: String, str2: String): String = {
    (str1, str2) match {
      case (s1: String, s2: String) => Seq(s1, s2).filter(_ != "").mkString(sep)
      case (null, s: String) => s
      case (s: String, null) => s
      case _ => ""
    }
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val acc1 = buffer.getAs[String](0)
    val acc2 = input.getAs[String](0)
    buffer(0) = concatStrings(acc1, acc2)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val acc1 = buffer1.getAs[String](0)
    val acc2 = buffer2.getAs[String](0)
    buffer1(0) = concatStrings(acc1, acc2)
  }

  override def evaluate(buffer: Row): Any = buffer.getAs[String](0)
}
