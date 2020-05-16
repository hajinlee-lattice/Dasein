package com.latticeengines.spark.aggregation


import com.latticeengines.common.exposed.util.JsonUtils
import com.latticeengines.domain.exposed.datacloud.dataflow.{DiscreteBucket, IntervalBucket}
import com.latticeengines.spark.util.NumericProfiler
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{StructField, _}

import scala.collection.JavaConverters._
import scala.util.Random

private[spark] class NumberProfileAggregation(fields: Seq[StructField], maxDiscrete: Int, numBuckets: Int, minBucketSize: Int, randomSeed: Long) extends UserDefinedAggregateFunction {

  private val NUM_SAMPLES: Int = 100000
  private val RANDOM = new Random(randomSeed)

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
    if (newVal != null) {
      val lst = buffer ++ Seq(newVal)
      if (lst.length >= 2 * NUM_SAMPLES) {
        return RANDOM.shuffle(lst).take(NUM_SAMPLES)
      } else {
        return lst
      }
    }
    buffer
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
    val lst = lst1 ++ lst2
    if (lst.length >= 2 * NUM_SAMPLES) {
      RANDOM.shuffle(lst).take(NUM_SAMPLES)
    } else {
      lst
    }
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    fields.indices.map(idx => {
      val field: StructField = fields(idx)
      val attr = field.name
      val vals = field.dataType match {
        case IntegerType => buffer.getList[Int](idx).asScala.toList.sorted
        case LongType => buffer.getList[Long](idx).asScala.toList.sorted
        case FloatType => buffer.getList[Float](idx).asScala.toList.sorted
        case DoubleType => buffer.getList[Double](idx).asScala.toList.sorted
        case _ => throw new UnsupportedOperationException(s"Non numeric type ${field.dataType}")
      }
      val distinctVals = field.dataType match {
        case IntegerType => buffer.getList[Int](idx).asScala.distinct.toList.sorted
        case LongType => buffer.getList[Long](idx).asScala.distinct.toList.sorted
        case FloatType => buffer.getList[Float](idx).asScala.distinct.toList.sorted
        case DoubleType => buffer.getList[Double](idx).asScala.distinct.toList.sorted
        case _ => throw new UnsupportedOperationException(s"Non numeric type ${field.dataType}")
      }
      if (distinctVals.size > maxDiscrete) {
        val profiler = field.dataType match {
          case IntegerType => new NumericProfiler[Int](buffer.getSeq[Int](idx), numBuckets, minBucketSize, randomSeed)
          case LongType => new NumericProfiler[Long](buffer.getSeq[Long](idx), numBuckets, minBucketSize, randomSeed)
          case FloatType => new NumericProfiler[Float](buffer.getSeq[Float](idx), numBuckets, minBucketSize, randomSeed)
          case DoubleType => new NumericProfiler[Double](buffer.getSeq[Double](idx), numBuckets, minBucketSize, randomSeed)
          case _ => throw new UnsupportedOperationException(s"Non numeric type ${field.dataType}")
        }
        val bnds: List[Double] = profiler.findBoundaries()
        val bkt = new IntervalBucket
        field.dataType match {
          case IntegerType => bkt.setBoundaries(bnds.map(_.toInt).asJava.asInstanceOf[java.util.List[Number]])
          case LongType => bkt.setBoundaries(bnds.map(_.toLong).asJava.asInstanceOf[java.util.List[Number]])
          case FloatType => bkt.setBoundaries(bnds.map(_.toFloat).asJava.asInstanceOf[java.util.List[Number]])
          case DoubleType => bkt.setBoundaries(bnds.asJava.asInstanceOf[java.util.List[Number]])
          case _ => throw new UnsupportedOperationException(s"Non numeric type ${field.dataType}")
        }
        Row.fromSeq(List(attr, JsonUtils.serialize(bkt)))
      } else {
        val bkt = new DiscreteBucket
        bkt.setValues(distinctVals.asJava.asInstanceOf[java.util.List[Number]])
        Row.fromSeq(List(attr, JsonUtils.serialize(bkt)))
      }
    })
  }

}
