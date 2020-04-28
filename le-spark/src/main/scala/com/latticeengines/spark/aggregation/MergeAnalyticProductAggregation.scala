package com.latticeengines.spark.aggregation

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.metadata.transaction.ProductStatus
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

private[spark] class MergeAnalyticProductAggregation extends UserDefinedAggregateFunction {

  private val Id = InterfaceName.Id.name
  private val Description = InterfaceName.Description.name
  private val Bundle = InterfaceName.ProductBundle.name
  private val Status = InterfaceName.ProductStatus.name
  private val Active = ProductStatus.Active.name
  private val Messages = "Messages"
  private val Priority = "Priority"

  private val idIdx = 0
  private val bundleIdx = 1
  private val descIdx = 2
  private val statusIdx = 3
  private val priorityIdx = 4
  private val msgIdx = 5

  override def inputSchema: StructType = StructType(List(
    StructField(Id, StringType),
    StructField(Bundle, StringType),
    StructField(Description, StringType),
    StructField(Status, StringType),
    StructField(Priority, IntegerType)
  ))

  override def bufferSchema: StructType = StructType(List(
    StructField(Id, StringType),
    StructField(Bundle, StringType),
    StructField(Description, StringType),
    StructField(Status, StringType),
    StructField(Priority, IntegerType),
    StructField(Messages, ArrayType(StringType))
  ))

  override def dataType: DataType = StructType(List(
    StructField(Id, StringType),
    StructField(Bundle, StringType),
    StructField(Description, StringType),
    StructField(Status, StringType),
    StructField(Messages, ArrayType(StringType))
  ))

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = null
    buffer(1) = null
    buffer(2) = null
    buffer(3) = null
    buffer(4) = 99
    buffer(5) = List()
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val oldPrio = buffer.getInt(priorityIdx)
    val newPrio = input.getInt(priorityIdx)
    if (newPrio >= oldPrio) {
      val status = input.get(statusIdx)
      if (Active.equals(status)) {
        val oldId = buffer.getString(idIdx)
        val newId = buffer.getString(idIdx)
        val message = s"Conflicting bundle $newId: ${input.mkString(", ")}"
        buffer(msgIdx) = message :: buffer.getList[String](msgIdx).asScala.toList
      }
    } else {
      copyToBuffer(buffer, input)
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    update(buffer1, buffer2)
  }

  override def evaluate(buffer: Row): Any = {
    Row.fromSeq(Seq(
      buffer.get(idIdx),
      buffer.get(bundleIdx),
      buffer.get(descIdx),
      buffer.get(statusIdx),
      buffer.get(msgIdx)
    ))
  }

  private def copyToBuffer(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(priorityIdx) = input.getInt(priorityIdx)
    buffer(idIdx) = input.getString(idIdx)
    buffer(bundleIdx) = input.getString(bundleIdx)
    buffer(descIdx) = input.getString(descIdx)

    val oldStatus = buffer.getString(statusIdx)
    val newStatus = input.getString(statusIdx)
    if (!Active.equals(oldStatus) && newStatus != null) {
      buffer(statusIdx) = newStatus
    }
  }

}
