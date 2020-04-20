package com.latticeengines.spark.aggregation

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.metadata.transaction.ProductStatus
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

private[spark] class MergeSpendingProductAggregation extends UserDefinedAggregateFunction {

  private val Name = InterfaceName.ProductName.name
  private val Line = InterfaceName.ProductLine.name
  private val Family = InterfaceName.ProductFamily.name
  private val Category = InterfaceName.ProductCategory.name
  private val LineId = InterfaceName.ProductLineId.name
  private val FamilyId = InterfaceName.ProductFamilyId.name
  private val CategoryId = InterfaceName.ProductCategoryId.name
  private val Status = InterfaceName.ProductStatus.name
  private val Active = ProductStatus.Active.name
  private val Messages = "Messages"

  private val nameIdx = 0
  private val lineIdx = 1
  private val famIdx = 2
  private val catIdx = 3

  private val statusIdx = 7
  private val msgIdx = 8

  override def inputSchema: StructType = StructType(List(
    StructField(Name, StringType),
    StructField(Line, StringType),
    StructField(Family, StringType),
    StructField(Category, StringType),
    StructField(LineId, StringType),
    StructField(FamilyId, StringType),
    StructField(CategoryId, StringType),
    StructField(Status, StringType)
  ))

  override def bufferSchema: StructType = StructType(List(
    StructField(Name, StringType),

    StructField(Line, StringType),
    StructField(Family, StringType),
    StructField(Category, StringType),

    StructField(LineId, StringType),
    StructField(FamilyId, StringType),
    StructField(CategoryId, StringType),

    StructField(Status, StringType),
    StructField(Messages, ArrayType(StringType))
  ))

  override def dataType: DataType = bufferSchema

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = null

    buffer(1) = null
    buffer(2) = null
    buffer(3) = null

    buffer(4) = null
    buffer(5) = null
    buffer(6) = null

    buffer(7) = null
    buffer(8) = List()
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val oldName = buffer.getString(nameIdx)
    val newName = input.getString(nameIdx)
    if (StringUtils.isBlank(oldName)) {
      buffer(nameIdx) = newName
    }

    updateIdx(buffer, input, lineIdx,
      (oldVal, newVal, input) => s"Conflicting line name $newVal and $oldVal: ${input.mkString(", ")}")

    updateIdx(buffer, input, famIdx,
      (oldVal, newVal, input) => s"Conflicting family name $newVal and $oldVal: ${input.mkString(", ")}")

    updateIdx(buffer, input, catIdx,
      (oldVal, newVal, input) => s"Conflicting category name $newVal and $oldVal: ${input.mkString(", ")}")

    val oldStatus = buffer.getString(statusIdx)
    val newStatus = input.getString(statusIdx)
    if (!Active.equals(oldStatus) && newStatus != null) {
      buffer(statusIdx) = newStatus
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    update(buffer1, buffer2)
    buffer1(msgIdx) = (buffer1.getList[String](msgIdx).asScala ++ buffer2.getList[String](msgIdx).asScala).toList
  }

  override def evaluate(buffer: Row): Any = buffer

  private def updateIdx(buffer: MutableAggregationBuffer, input: Row, idx: Int,
                        errorMsgBldr: Function3[String, String, Row, String]): Unit = {
    val oldVal = buffer.getString(idx)
    val newVal = input.getString(idx)
    if (StringUtils.isBlank(oldVal)) {
      buffer(idx) = newVal
      buffer(idx + 3) = input.getString(idx + 3)
    } else if (StringUtils.isNotBlank(newVal) && !oldVal.equalsIgnoreCase(newVal)) {
      val message = errorMsgBldr.apply(oldVal, newVal, input)
      buffer(msgIdx) = message :: buffer.getList[String](msgIdx).asScala.toList
    }
  }

}
