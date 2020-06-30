package com.latticeengines.spark.aggregation

import com.latticeengines.common.exposed.util.JsonUtils
import com.latticeengines.domain.exposed.datacloud.dataflow.{CategoricalBucket, DiscreteBucket}
import com.latticeengines.domain.exposed.spark.common.ChangeListConstants
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

private[spark] class UpdateProfileAggregation(valMap: Broadcast[Map[String, Seq[Any]]], maxCategories: Int, maxLength: Int, maxDiscrete: Int) extends UserDefinedAggregateFunction {

  private val StringVals = "StringVals"
  private val IntegerVals = "IntegerVals"
  private val LongVals = "LongVals"
  private val FloatVals = "FloatVals"
  private val DoubleVals = "DoubleVals"

  private val Initialized = "Initialized"
  private val Exceeded = "Exceeded"

  private val typeIdx = 0;
  private val stringIdx = 1;
  private val intIdx = 2;
  private val longIdx = 3;
  private val floatIdx = 4;
  private val doubleIdx = 5;

  private val attrIdx = 6;

  private val initIdx = 6;
  private val exceededIdx = 7;

  override def inputSchema: StructType = StructType(List(
    StructField(ChangeListConstants.DataType, StringType, nullable = false),
    StructField(ChangeListConstants.ToString, StringType, nullable = true),
    StructField(ChangeListConstants.ToInteger, IntegerType, nullable = true),
    StructField(ChangeListConstants.ToLong, LongType, nullable = true),
    StructField(ChangeListConstants.ToFloat, FloatType, nullable = true),
    StructField(ChangeListConstants.ToDouble, DoubleType, nullable = true),
    StructField(ChangeListConstants.ColumnId, StringType, nullable = false)
  ))

  override def bufferSchema: StructType = StructType(List(
    StructField(ChangeListConstants.DataType, StringType),
    StructField(StringVals, ArrayType(StringType)),
    StructField(IntegerVals, ArrayType(IntegerType)),
    StructField(LongVals, ArrayType(LongType)),
    StructField(FloatVals, ArrayType(FloatType)),
    StructField(DoubleVals, ArrayType(DoubleType)),
    StructField(Initialized, BooleanType),
    StructField(Exceeded, BooleanType)
  ))

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(typeIdx) = null
    buffer(stringIdx) = Seq()
    buffer(intIdx) = Seq()
    buffer(longIdx) = Seq()
    buffer(floatIdx) = Seq()
    buffer(doubleIdx) = Seq()
    buffer(initIdx) = false
    buffer(exceededIdx) = false
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val dType = input.getString(typeIdx)
    if (!buffer.getBoolean(initIdx)) {
      val attrName = input.getString(attrIdx)
      buffer(typeIdx) = dType
      val values: Seq[Any] = valMap.value(attrName)
      dType match {
        case "Integer" => buffer(intIdx) = values.map({
          case x: Int => x
          case x: Long => x.intValue
          case x: Float => x.intValue
          case x: Double => x.intValue
        })
        case "Long" => buffer(longIdx) = values.map({
          case x: Int => x.longValue
          case x: Long => x
          case x: Float => x.longValue
          case x: Double => x.longValue
        })
        case "Float" => buffer(floatIdx) = values.map({
          case x: Int => x.floatValue
          case x: Long => x.floatValue
          case x: Float => x
          case x: Double => x.floatValue
        })
        case "Double" => buffer(doubleIdx) = values.map({
          case x: Int => x.doubleValue
          case x: Long => x.doubleValue
          case x: Float => x.doubleValue
          case x: Double => x
        })
        case "String" => buffer(stringIdx) = values.map({
          case x: String => x
          case x: Int => x.toString
          case x: Long => x.toString
          case x: Float => x.toString
          case x: Double => x.toString
        })
        case _ => throw new IllegalArgumentException(s"Invalid data type: $dType")
      }
      buffer(initIdx) = true
    }

    if (!buffer.getBoolean(exceededIdx)) {
      val valObj = dType match {
        case "Integer" => input.getInt(intIdx)
        case "Long" => input.getLong(longIdx)
        case "Float" => input.getFloat(floatIdx)
        case "Double" => input.getDouble(doubleIdx)
        case "String" => input.getString(stringIdx)
        case _ => throw new IllegalArgumentException(s"Invalid data type: $dType")
      }
      val isNew = dType match {
        case "Integer" => valObj != null && !buffer.getSeq[Int](intIdx).contains(valObj)
        case "Long" => valObj != null && !buffer.getSeq[Long](longIdx).contains(valObj)
        case "Float" => valObj != null && !buffer.getSeq[Float](floatIdx).contains(valObj)
        case "Double" => valObj != null && !buffer.getSeq[Double](doubleIdx).contains(valObj)
        case "String" => valObj != null && !buffer.getSeq[String](stringIdx).contains(valObj)
        case _ => throw new IllegalArgumentException(s"Invalid data type: $dType")
      }
      if (isNew) {
        dType match {
          case "Integer" => buffer(intIdx) = buffer.getSeq[Int](intIdx) ++ List(valObj)
          case "Long" => buffer(longIdx) = buffer.getSeq[Long](longIdx) ++ List(valObj)
          case "Float" => buffer(floatIdx) = buffer.getSeq[Float](floatIdx) ++ List(valObj)
          case "Double" => buffer(doubleIdx) = buffer.getSeq[Double](doubleIdx) ++ List(valObj)
          case "String" => buffer(stringIdx) = buffer.getSeq[String](stringIdx) ++ List(valObj)
          case _ => throw new IllegalArgumentException(s"Invalid data type: $dType")
        }
        buffer(exceededIdx) = dType match {
          case "Integer" => buffer.getSeq[Int](intIdx).size > maxDiscrete
          case "Long" => buffer.getSeq[Long](longIdx).size > maxDiscrete
          case "Float" => buffer.getSeq[Float](floatIdx).size > maxDiscrete
          case "Double" => buffer.getSeq[Double](doubleIdx).size > maxDiscrete
          case "String" => valObj.asInstanceOf[String].length > maxLength ||
            buffer.getSeq[String](stringIdx).size > maxCategories
          case _ => throw new IllegalArgumentException(s"Invalid data type: $dType")
        }
      }
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if (buffer1.getBoolean(exceededIdx) || buffer2.getBoolean(exceededIdx)) {
      buffer1(exceededIdx) = true
    } else if (!buffer1.getBoolean(initIdx)) {
      buffer1(typeIdx) = buffer2.getString(typeIdx)
      buffer1(stringIdx) = buffer2.getSeq[String](stringIdx)
      buffer1(intIdx) = buffer2.getSeq[Int](intIdx)
      buffer1(longIdx) = buffer2.getSeq[Long](longIdx)
      buffer1(floatIdx) = buffer2.getSeq[Float](floatIdx)
      buffer1(doubleIdx) = buffer2.getSeq[Double](doubleIdx)
      buffer1(initIdx) = buffer2.getBoolean(initIdx)
      buffer1(exceededIdx) = buffer2.getBoolean(exceededIdx)
    } else if (buffer2.getBoolean(initIdx)) {
      val dType = buffer1.getString(typeIdx)
      val valObjs = dType match {
        case "Integer" => buffer2.getSeq[Int](intIdx)
        case "Long" => buffer2.getSeq[Long](longIdx)
        case "Float" => buffer2.getSeq[Float](floatIdx)
        case "Double" => buffer2.getSeq[Double](doubleIdx)
        case "String" => buffer2.getSeq[String](stringIdx)
        case _ => throw new IllegalArgumentException(s"Invalid data type: $dType")
      }
      val strTooLong = dType match {
        case "String" => buffer2.getSeq[String](stringIdx).map(_.length).max > maxLength
        case _ => false
      }
      dType match {
        case "Integer" => buffer1(intIdx) = buffer1.getSeq[Int](intIdx) ++ valObjs.diff(buffer1.getSeq[Int](intIdx))
        case "Long" => buffer1(longIdx) = buffer1.getSeq[Long](longIdx) ++ valObjs.diff(buffer1.getSeq[Long](longIdx))
        case "Float" => buffer1(floatIdx) = buffer1.getSeq[Float](floatIdx) ++ valObjs.diff(buffer1.getSeq[Float](floatIdx))
        case "Double" => buffer1(doubleIdx) = buffer1.getSeq[Double](doubleIdx) ++ valObjs.diff(buffer1.getSeq[Double](doubleIdx))
        case "String" => buffer1(stringIdx) = buffer1.getSeq[String](stringIdx) ++ valObjs.diff(buffer1.getSeq[String](stringIdx))
        case _ => throw new IllegalArgumentException(s"Invalid data type: $dType")
      }
      buffer1(exceededIdx) = dType match {
        case "Integer" => buffer1.getSeq[Int](intIdx).size > maxDiscrete
        case "Long" => buffer1.getSeq[Long](longIdx).size > maxDiscrete
        case "Float" => buffer1.getSeq[Float](floatIdx).size > maxDiscrete
        case "Double" => buffer1.getSeq[Double](doubleIdx).size > maxDiscrete
        case "String" => strTooLong || buffer1.getSeq[String](stringIdx).size > maxCategories
        case _ => throw new IllegalArgumentException(s"Invalid data type: $dType")
      }
    }
  }

  override def evaluate(buffer: Row): Any = {
    if (buffer.getBoolean(exceededIdx)) {
      null
    } else {
      val dType = buffer.getString(typeIdx)
      dType match {
        case "Integer" =>
          val bkt: DiscreteBucket = new DiscreteBucket
          bkt.setValues(buffer.getSeq[Int](intIdx).sorted.asJava.asInstanceOf[java.util.List[Number]])
          JsonUtils.serialize(bkt)
        case "Long" =>
          val bkt: DiscreteBucket = new DiscreteBucket
          bkt.setValues(buffer.getSeq[Long](longIdx).sorted.asJava.asInstanceOf[java.util.List[Number]])
          JsonUtils.serialize(bkt)
        case "Float" =>
          val bkt: DiscreteBucket = new DiscreteBucket
          bkt.setValues(buffer.getSeq[Float](floatIdx).sorted.asJava.asInstanceOf[java.util.List[Number]])
          JsonUtils.serialize(bkt)
        case "Double" =>
          val bkt: DiscreteBucket = new DiscreteBucket
          bkt.setValues(buffer.getSeq[Double](doubleIdx).sorted.asJava.asInstanceOf[java.util.List[Number]])
          JsonUtils.serialize(bkt)
        case "String" =>
          val bkt: CategoricalBucket = new CategoricalBucket
          bkt.setCategories(buffer.getSeq[String](stringIdx).asJava)
          JsonUtils.serialize(bkt)
        case _ =>
          throw new IllegalArgumentException(s"Invalid data type: $dType")
      }
    }
  }

}
