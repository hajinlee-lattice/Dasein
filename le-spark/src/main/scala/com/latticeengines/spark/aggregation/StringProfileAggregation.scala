package com.latticeengines.spark.aggregation

import com.latticeengines.common.exposed.util.JsonUtils
import com.latticeengines.domain.exposed.datacloud.dataflow.CategoricalBucket
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{StructField, _}

import scala.collection.JavaConverters._

private[spark] class StringProfileAggregation(fields: Seq[String], maxCategories: Int, maxLength: Int) extends UserDefinedAggregateFunction {

  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(fields.map(StructField(_, StringType, nullable = false)))

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(fields.map(field => {
    StructField(field, StructType(List(
      StructField("categories", ArrayType(StringType)),
      StructField("distinct_values", IntegerType)
    )))
  }))

  // This is the output type of your aggregatation function.
  override def dataType: DataType = ArrayType(StructType(List(
    StructField("attr", StringType, nullable = false),
    StructField("algo", StringType, nullable = true)
  )))

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    fields.indices.foreach(idx => {
      buffer(idx) = Row.fromSeq(List(List(), 0))
    })
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    fields.indices.foreach(idx => {
      val currRow: Row = buffer.getAs[Row](idx)
      if (currRow.getInt(1) <= maxCategories) { // not exploded yet
        val v = input.getAs[String](idx)
        val newVal = if(v == null) null else v.trim
        if (newVal != null && newVal.length > 0) {
          if (newVal.length > maxLength) {
            // explode due to long string
            buffer(idx) = Row.fromSeq(List(List(), maxCategories + 1))
          } else {
            val cats: List[String] = currRow.getList[String](0).asScala.toList
            if (!cats.contains(newVal)) { // merge new value
              val newCats: List[String] = newVal :: cats
              val newCnt = newCats.length
              if (newCnt <= maxCategories) {
                buffer(idx) = Row.fromSeq(List(newCats, newCnt))
              } else {
                buffer(idx) = Row.fromSeq(List(List(), newCnt))
              }
            }
          }
        }
      }
    })
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    fields.indices.foreach(idx => {
      val row1 = buffer1.getAs[Row](idx)
      val row2 = buffer2.getAs[Row](idx)
      val cnt1 = row1.getInt(1)
      val cnt2 = row2.getInt(1)
      if (cnt1 <= maxCategories && cnt2 <= maxCategories) {
        val lst1 = row1.getList[String](0).asScala.toSet
        val lst2 = row2.getList[String](0).asScala.toSet
        val lst = (lst1 ++ lst2).toList
        val cnt = lst.length
        if (cnt <= maxCategories) {
          buffer1(idx) = Row.fromSeq(List(lst, cnt))
        } else {
          buffer1(idx) = Row.fromSeq(List(List(), cnt))
        }
      } else {
        buffer1(idx) = Row.fromSeq(List(List(), maxCategories + 1))
      }
    })
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    fields.indices.map(idx => {
      val field = fields(idx)
      val row = buffer.getAs[Row](idx)
      val cnt = row.getInt(1)
      if (cnt > maxCategories) {
        Row.fromSeq(List(field, null))
      } else {
        val bkt: CategoricalBucket = new CategoricalBucket
        bkt.setCategories(row.getList[String](0))
        Row.fromSeq(List(field, JsonUtils.serialize(bkt)))
      }
    })
  }

}
