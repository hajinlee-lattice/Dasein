package com.latticeengines.spark.aggregation

import com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_BKTS
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, MapType, StringType, StructField, StructType}

private[spark] class BucketCountAggregation extends UserDefinedAggregateFunction {

  private val Value = "Value"
  private val BktId = "BktId"
  private val Count = "Count"
  private val CountMap = "CountMap"
  private val BktSum = "BktSum"
  private val Bkts = STATS_ATTR_BKTS

  override def inputSchema: StructType = StructType(List(
    StructField(BktId, IntegerType),
    StructField(Count, LongType)
  ))

  override def bufferSchema: StructType = StructType(List(
    StructField(CountMap, MapType(IntegerType, LongType))
  ))

  override def dataType: DataType = StructType(List(
    StructField(BktSum, LongType),
    StructField(Bkts, StringType)
  ))

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map()
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val newBkt = input.getInt(0)
    val newCnt = input.getLong(1)
    val oldMap = buffer.getMap[Int, Long](0)
    val bkts = (newBkt :: oldMap.keys.toList).distinct
    buffer(0) = bkts.map(bkt => {
      val oldCnt = oldMap.getOrElse(bkt, 0L)
      if (newBkt == bkt) {
        (bkt, oldCnt + newCnt)
      } else {
        (bkt, oldCnt)
      }
    }).toMap[Int, Long]
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val map1 = buffer1.getMap[Int, Long](0)
    val map2 = buffer2.getMap[Int, Long](0)
    val bkts = (map1.keys.toList union map2.keys.toList).distinct
    buffer1(0) = bkts.map(bkt => {
      val cnt1 = map1.getOrElse(bkt, 0L)
      val cnt2 = map2.getOrElse(bkt, 0L)
      (bkt, cnt1 + cnt2)
    }).toMap[Int, Long]
  }

  override def evaluate(buffer: Row): Any = {
    val cntMap = buffer.getMap[Int, Long](0)
    val bkts = cntMap.map(t => {
      val (bkt, cnt) = t
      s"$bkt:$cnt"
    }).toList.sortBy(token => token.split(":")(0).toInt).mkString("|")
    val cnt = cntMap.filter(t => t._1 != 0).values.sum
    Row.fromSeq(Seq(cnt, bkts))
  }

}
