package com.latticeengines.spark.aggregation

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.metadata.transaction.ProductStatus
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

private[spark] class MergeSpendingProductAggregation extends UserDefinedAggregateFunction {

  private val Id = InterfaceName.Id.name
  private val ProductId = InterfaceName.ProductId.name
  private val Name = InterfaceName.ProductName.name
  private val Description = InterfaceName.Description.name
  private val Type = InterfaceName.ProductType.name
  private val Bundle = InterfaceName.ProductBundle.name
  private val Line = InterfaceName.ProductLine.name
  private val Family = InterfaceName.ProductFamily.name
  private val Category = InterfaceName.ProductCategory.name
  private val BundleId = InterfaceName.ProductBundleId.name
  private val LineId = InterfaceName.ProductLineId.name
  private val FamilyId = InterfaceName.ProductFamilyId.name
  private val CategoryId = InterfaceName.ProductCategoryId.name
  private val Status = InterfaceName.ProductStatus.name

  private val Active = ProductStatus.Active.name
  private val Obsolete = ProductStatus.Obsolete.name

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
    val oldName = buffer.getAs[String](nameIdx)
    val newName = input.getAs[String](nameIdx)
    if (StringUtils.isBlank(oldName)) {
      buffer(nameIdx) = newName
    }

    val oldLine = buffer.getAs[String](lineIdx)
    val newLine = input.getAs[String](lineIdx)
    if (StringUtils.isBlank(oldLine)) {
      buffer(lineIdx) = newLine
      buffer(lineIdx + 3) = input.getAs[String](lineIdx + 3)
    } else if (StringUtils.isNotBlank(newLine) && !oldLine.equalsIgnoreCase(newLine)) {
      val message = s"Conflicting line name $newLine and $oldLine: ${input.mkString(", ")}"
      buffer(msgIdx) = message :: buffer.getList[String](msgIdx).asScala.toList
    }

    val oldFam = buffer.getAs[String](famIdx)
    val newFam = input.getAs[String](famIdx)
    if (StringUtils.isBlank(oldFam)) {
      buffer(famIdx) = newFam
      buffer(famIdx + 3) = input.getAs[String](famIdx + 3)
    } else if (StringUtils.isNotBlank(newFam) && !oldFam.equalsIgnoreCase(newFam)) {
      val message = s"Conflicting family name $newFam and $oldFam: ${input.mkString(", ")}"
      buffer(msgIdx) = message :: buffer.getList[String](msgIdx).asScala.toList
    }

    val oldCat = buffer.getAs[String](catIdx)
    val newCat = input.getAs[String](catIdx)
    if (StringUtils.isBlank(oldCat)) {
      buffer(catIdx) = newCat
      buffer(catIdx + 3) = input.getAs[String](catIdx + 3)
    } else if (StringUtils.isNotBlank(newCat) && !oldCat.equalsIgnoreCase(newCat)) {
      val message = s"Conflicting category name $newCat and $oldCat: ${input.mkString(", ")}"
      buffer(msgIdx) = message :: buffer.getList[String](msgIdx).asScala.toList
    }

    val oldStatus = buffer.getAs[String](statusIdx)
    val newStatus = input.getAs[String](statusIdx)
    if (!Active.equals(oldStatus) && newStatus != null) {
      buffer(statusIdx) = newStatus
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    update(buffer1, buffer2)
    buffer1(msgIdx) = (buffer1.getList[String](msgIdx).asScala ++ buffer2.getList[String](msgIdx).asScala).toList
  }

  override def evaluate(buffer: Row): Any = buffer
}
