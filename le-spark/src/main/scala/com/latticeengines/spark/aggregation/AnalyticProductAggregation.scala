package com.latticeengines.spark.aggregation

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.metadata.transaction.ProductStatus
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

private[spark] class AnalyticProductAggregation extends UserDefinedAggregateFunction {

   private val Description = InterfaceName.Description.name
  private val Bundle = InterfaceName.ProductBundle.name
  private val Status = InterfaceName.ProductStatus.name
  private val Active = ProductStatus.Active.name
  private val Messages = "Messages"

  override def inputSchema: StructType = StructType(List(
    StructField(Bundle, StringType),
    StructField(Description, StringType),
    StructField(Status, StringType)
  ))

  override def bufferSchema: StructType = StructType(List(
    StructField(Bundle, StringType),
    StructField(Description, StringType),
    StructField(Status, StringType),
    StructField(Messages, ArrayType(StringType))
  ))

  override def dataType: DataType = StructType(List(
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
    buffer(3) = List()
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val oldBundle = buffer.getAs[String](0)
    val newBundle = input.getAs[String](0)
    if (StringUtils.isBlank(oldBundle)) {
      buffer(0) = newBundle
    } else if (StringUtils.isNotBlank(newBundle) && !oldBundle.equalsIgnoreCase(newBundle)) {
      val message = s"Conflicting bundle name $newBundle and $oldBundle: ${input.mkString(", ")}"
      buffer(3) = message :: buffer.getList[String](3).asScala.toList
    }

    val newDesc = input.getAs[String](1)
    if (StringUtils.isNotBlank(newDesc)) {
      buffer(1) = newDesc
    }

    val oldStatus = buffer.getAs[String](2)
    val newStatus = input.getAs[String](2)
    if (!Active.equals(oldStatus) && newStatus != null) {
      buffer(2) = newStatus
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val oldBundle = buffer1.getAs[String](0)
    val newBundle = buffer2.getAs[String](0)
    val messages = (buffer1.getList[String](3).asScala ++ buffer2.getList[String](3).asScala).toList
    if (StringUtils.isBlank(oldBundle)) {
      buffer1(0) = newBundle
    } else if (StringUtils.isNotBlank(newBundle) && !oldBundle.equalsIgnoreCase(newBundle)) {
      val message = s"Conflicting bundle name $newBundle and $oldBundle: ${buffer2.mkString(", ")}"
      buffer1(3) = message :: messages
    }

    val newDesc = buffer2.getAs[String](1)
    if (StringUtils.isNotBlank(newDesc)) {
      buffer1(2) = newDesc
    }

    val oldStatus = buffer1.getAs[String](2)
    val newStatus = buffer2.getAs[String](2)
    if (!Active.equals(oldStatus) && newStatus != null) {
      buffer1(2) = newStatus
    }
  }

  override def evaluate(buffer: Row): Any = buffer
}
