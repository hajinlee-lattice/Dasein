package com.latticeengines.spark.aggregation


import com.latticeengines.common.exposed.util.JsonUtils
import com.latticeengines.domain.exposed.datacloud.dataflow.{DiscreteBucket, IntervalBucket}
import org.apache.commons.math3.stat.descriptive.moment.Kurtosis
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{StructField, _}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

private[aggregation] object NumberProfileAggregation {

  private val log: Logger = LoggerFactory.getLogger(classOf[NumberProfileAggregation])

  def roundTo5(x: Double): Double = {
    roundTo(x, sigDigits=1)
    if (math.abs(x)<= 10) {
      roundTo(x, sigDigits=1)
    } else {
      val x2 = roundTo(x, sigDigits = 2)
      val secondDigit = getSecondDigit(x2)
      val round5Set = Set(3, 4, 5, 6, 7)
      if (round5Set.contains(secondDigit)) {
        replaceSecondDigitByFive(x2)
      } else {
        roundTo(x, sigDigits=1)
      }
    }
  }

  def roundTo(x: Double, sigDigits: Int): Double = {
    val scale: Int = getScale(x, sigDigits)
    BigDecimal(x).setScale(scale, BigDecimal.RoundingMode.HALF_EVEN).toDouble
  }

  private def getSecondDigit(x: Double): Int = {
    val scale: Int = getScale(x, sigDigits = 2)
    math.floor(math.abs(x) * math.pow(10, scale)).toInt % 10
  }

  private def replaceSecondDigitByFive(x: Double): Double = {
    val scale: Int = getScale(x, sigDigits = 2)
    val xStr = String.valueOf(math.abs(x))
    val dotPos = xStr.indexOf(".")
    val replaced = if (xStr.contains("E")) {
      // 1.7E9
      val ePos = xStr.indexOf("E")
      (xStr.substring(0, dotPos) + ".5" + xStr.substring(ePos)).toDouble
    } else {
      val dotPos = xStr.indexOf(".")
      val digitPos = dotPos + scale - 1
      (xStr.substring(0, digitPos) + "5" + xStr.substring(digitPos + 1)).toDouble
    }
    if (x >= 0) replaced else -replaced
  }

  private def getScale(x: Double, sigDigits: Int): Int = {
    val digits: Int = math.log10(math.abs(x)).toInt
    if (digits == 0) {
      if (x >= 1) -digits + (sigDigits - 1) else -digits + sigDigits
    } else {
      if (digits > 0) -digits + (sigDigits - 1) else -digits + sigDigits
    }
  }

}

private[spark] class NumberProfileAggregation(fields: Seq[StructField], maxDiscrete: Int, numBuckets: Int, minBucketSize: Int, randomSeed: Long) extends UserDefinedAggregateFunction {

  private val NUM_SAMPLES: Int = 50000
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

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    fields.indices.foreach(idx => {
      buffer(idx) = List()
    })
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    fields.indices.foreach(idx => {
      val field: StructField = fields(idx)
      buffer(idx) = field.dataType match {
        case IntegerType => updateRow(buffer.getList[Int](idx).asScala.toList, input.getAs[Int](idx))
        case LongType => updateRow(buffer.getList[Long](idx).asScala.toList, input.getAs[Long](idx))
        case FloatType => updateRow(buffer.getList[Float](idx).asScala.toList, input.getAs[Float](idx))
        case DoubleType => updateRow(buffer.getList[Double](idx).asScala.toList, input.getAs[Double](idx))
        case _ => throw new UnsupportedOperationException(s"Non numeric type ${field.dataType}")
      }
    })
  }

  private def updateRow[T](buffer: List[T], newVal: T): List[T] = {
    if (newVal != null) {
      if (!buffer.contains(newVal)) {
        val newCats: List[T] = newVal :: buffer
        if (newCats.length >= 2 * NUM_SAMPLES) {
          return RANDOM.shuffle(newCats).splitAt(NUM_SAMPLES)._1
        } else {
          return newCats
        }
      }
    }
    buffer
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    fields.indices.foreach(idx => {
      val field: StructField = fields(idx)
      buffer1(idx) = field.dataType match {
        case IntegerType => mergeBuffers(buffer1.getList[Int](idx).asScala.toList, buffer2.getList[Int](idx).asScala.toList)
        case LongType => mergeBuffers(buffer1.getList[Long](idx).asScala.toList, buffer2.getList[Long](idx).asScala.toList)
        case FloatType => mergeBuffers(buffer1.getList[Float](idx).asScala.toList, buffer2.getList[Float](idx).asScala.toList)
        case DoubleType => mergeBuffers(buffer1.getList[Double](idx).asScala.toList, buffer2.getList[Double](idx).asScala.toList)
        case _ => throw new UnsupportedOperationException(s"Non numeric type ${field.dataType}")
      }
    })
  }

  private def mergeBuffers[T](lst1: List[T], lst2: List[T]): List[T] = {
    val lst = (lst1 ++ lst2).distinct
    if (lst.length >= 2 * NUM_SAMPLES) {
      RANDOM.shuffle(lst).splitAt(NUM_SAMPLES)._1
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
      if (vals.size > maxDiscrete) {
        val bkt = new IntervalBucket
        val bnds: List[Double] = findBoundaries(vals)
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
        bkt.setValues(vals.asJava.asInstanceOf[java.util.List[Number]])
        Row.fromSeq(List(attr, JsonUtils.serialize(bkt)))
      }
    })
  }

  private def findBoundaries[T](vals: List[T]): List[Double] = {
    val dVals = transform(vals)
    val bndIdx = findBoundaryIdx(dVals)
    toBoundaries(dVals, bndIdx)
  }

  private def toBoundaries[T](dVals: List[Double], bndIdx: List[Int]): List[Double] = {
    if (bndIdx.length <= 2) {
      findDistinctValueBoundaries(dVals)
    } else {
      bndIdx.slice(1, bndIdx.length - 1).map(dVals)
    }
  }

  private def findDistinctValueBoundaries[T](dVals: List[Double]): List[Double] = {
    val distinctVals = dVals.toSet.toList.sorted
    var sortedVals = if (distinctVals.length > 2) distinctVals.slice(1, distinctVals.length - 1) else distinctVals
    for (_ <- 0 until sortedVals.length - (numBuckets - 1)) {
      val (p1, p2) = sortedVals.splitAt(RANDOM.nextInt(sortedVals.length))
      sortedVals = p1 ++ p2.tail
    }
    sortedVals
  }

  /**
    * generate a list of small bins as a first step in bucketing. bins are
    * built from a sorted list. returns: a list of index positions at
    * boundaries of these bins
    */
  private def findBoundaryIdx(dVals: List[Double]): List[Int] = {
    // if there is not enough data, no bucketing is needed
    if (dVals.length <= minBucketSize) {
      List(0, dVals.length)
    } else {
      val vals = if (isGeoDist(dVals.toArray)) logTransform(dVals) else dVals

      val xMin = vals.head
      val xMax = vals.last
      val boundaries: Array[Double] = Array.ofDim[Double](numBuckets)
      for (i <- 1 until numBuckets) {
        boundaries(i - 1) = xMin + (xMax - xMin) * i / numBuckets
      }
      val bndIdx: mutable.MutableList[Int] = new mutable.MutableList[Int]
      bndIdx += 0
      for (b <- boundaries) {
        val bIdx = bisectLeft(vals, b)
        if ((bIdx - bndIdx.last) >= minBucketSize) {
          bndIdx += bIdx
        }
      }
      if (xMax > bndIdx.last - minBucketSize) {
        bndIdx += vals.length
      } else {
        bndIdx.update(bndIdx.size - 1, vals.length)
      }
      bndIdx.toList
    }
  }

  private def bisectLeft(vals: List[Double], key: Double): Int = {
    var bIdx: Int = java.util.Arrays.binarySearch(vals.toArray, key)
    if (bIdx >= 0) {
      while (bIdx > 0 && vals(bIdx) == vals(bIdx - 1)) bIdx -= 1
    } else {
      bIdx = -(bIdx + 1)
    }
    bIdx
  }

  private def transform[T](vals: List[T]): List[Double] = {
    vals.map {
      case v: Int => v.toDouble
      case v: Long => v.toDouble
      case v: Float => v.toDouble
      case v: Double => v
      case v => throw new UnsupportedOperationException(s"Unknown type ${v.getClass}")
    } map NumberProfileAggregation.roundTo5
  }

  private def isGeoDist(dVals: Array[Double]): Boolean = {
    val k = new Kurtosis
    k.evaluate(dVals) >= 3.0
  }

  private def logTransform(dVals:List[Double]): List[Double] = {
    dVals map (d => {
      if (d > 0) {
        math.log10(d)
      } else if (d < 0) {
          -math.log10(-d)
      } else {
        0
      }
    })
  }

}
