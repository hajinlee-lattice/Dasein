package com.latticeengines.spark.aggregation

import com.latticeengines.common.exposed.util.JsonUtils
import com.latticeengines.domain.exposed.datacloud.dataflow.{DiscreteBucket, IntervalBucket}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

private[spark] class DiscreteProfileAggregation(fields: Seq[StructField], maxDiscrete: Int) extends UserDefinedAggregateFunction {

  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType = StructType(fields)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(fields.map(field => {
    StructField(field.name, ArrayType(field.dataType))
  }))

  // This is the output type of your aggregation function.
  override def dataType: DataType = ArrayType(StructType(List(
    StructField("attr", StringType, nullable = false),
    StructField("algo", StringType, nullable = true)
  )))

  override def deterministic: Boolean = false

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    fields.indices.foreach(idx => buffer(idx) = Seq())
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    fields.indices.foreach(idx => {
      val field: StructField = fields(idx)
      buffer(idx) = field.dataType match {
        case IntegerType => updateRow(buffer.getSeq[Int](idx), input.getAs[Int](idx))
        case LongType => updateRow(buffer.getSeq[Long](idx), input.getAs[Long](idx))
        case FloatType => updateRow(buffer.getSeq[Float](idx), input.getAs[Float](idx))
        case DoubleType => updateRow(buffer.getSeq[Double](idx), input.getAs[Double](idx))
        case _ => throw new UnsupportedOperationException(s"Non numeric type ${field.dataType}")
      }
    })
  }

  private def updateRow[T](buffer: Seq[T], newVal: T): Seq[T] = {
    if (buffer.length <= maxDiscrete && newVal != null && !buffer.contains(newVal)) {
      buffer ++ Seq(newVal)
    } else {
      buffer
    }
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    fields.indices.foreach(idx => {
      val field: StructField = fields(idx)
      buffer1(idx) = field.dataType match {
        case IntegerType => mergeBuffers(buffer1.getSeq[Int](idx), buffer2.getSeq[Int](idx))
        case LongType => mergeBuffers(buffer1.getSeq[Long](idx), buffer2.getSeq[Long](idx))
        case FloatType => mergeBuffers(buffer1.getSeq[Float](idx), buffer2.getSeq[Float](idx))
        case DoubleType => mergeBuffers(buffer1.getSeq[Double](idx), buffer2.getSeq[Double](idx))
        case _ => throw new UnsupportedOperationException(s"Non numeric type ${field.dataType}")
      }
    })
  }

  private def mergeBuffers[T](lst1: Seq[T], lst2: Seq[T]): Seq[T] = {
    if (lst1.length <= maxDiscrete && lst2.length <= maxDiscrete) {
      (lst1 ++ lst2).distinct.take(maxDiscrete + 1)
    } else if (lst1.length > maxDiscrete) {
      lst1
    } else {
      lst2
    }
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    fields.indices.map(idx => {
      val field: StructField = fields(idx)
      val attr = field.name
      val distinctVals = field.dataType match {
        case IntegerType => buffer.getSeq[Int](idx).distinct.toList.sorted
        case LongType => buffer.getSeq[Long](idx).distinct.toList.sorted
        case FloatType => buffer.getSeq[Float](idx).distinct.toList.sorted
        case DoubleType => buffer.getSeq[Double](idx).distinct.toList.sorted
        case _ => throw new UnsupportedOperationException(s"Non numeric type ${field.dataType}")
      }
      if (distinctVals.size > maxDiscrete) {
        val bkt = new IntervalBucket
        Row.fromSeq(List(attr, JsonUtils.serialize(bkt)))
      } else {
        val bkt = new DiscreteBucket
        bkt.setValues(distinctVals.asJava.asInstanceOf[java.util.List[Number]])
        Row.fromSeq(List(attr, JsonUtils.serialize(bkt)))
      }
    })
  }

}
