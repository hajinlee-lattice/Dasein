package com.latticeengines.spark.aggregation

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.metadata.transaction.ProductStatus
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

private[spark] class DedupBundleProductAggregation extends UserDefinedAggregateFunction {

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

  private val idIdx = 0
  private val nameIdx = 1
  private val descIdx = 2
  private val bundleIdx = 3
  private val msgIdx = 4
  private val flagIdx = 5

  override def inputSchema: StructType = StructType(List(
    StructField(Id, StringType),
    StructField(Name, StringType),
    StructField(Description, StringType),
    StructField(Bundle, StringType)
  ))

  override def bufferSchema: StructType = StructType(List(
    StructField(Id, StringType),
    StructField(Name, StringType),
    StructField(Description, StringType),
    StructField(Bundle, StringType),
    StructField(Messages, ArrayType(StringType)),
    StructField("first", BooleanType)
  ))

  override def dataType: DataType = StructType(List(
    StructField(Name, StringType),
    StructField(Description, StringType),
    StructField(Bundle, StringType),
    StructField(Messages, ArrayType(StringType))
  ))

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = null
    buffer(1) = null
    buffer(2) = null
    buffer(3) = null
    buffer(4) = List()
    buffer(5) = true
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val isFirst = buffer.getBoolean(flagIdx)

    if (isFirst) {
      buffer(idIdx) = input.getString(idIdx)
      buffer(nameIdx) = input.getString(nameIdx)
      buffer(descIdx) = input.getString(descIdx)
      buffer(bundleIdx) = input.getString(bundleIdx)
      buffer(flagIdx) = false
    } else {
      val sku = input.getString(idIdx)
      val bundle = input.getString(bundleIdx)
      val message = s"Duplicated sku in bundle data: $sku -> $bundle"
      buffer(msgIdx) = message :: buffer.getList[String](msgIdx).asScala.toList
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val isFirst1 = buffer1.getBoolean(flagIdx)
    val isFirst2 = buffer2.getBoolean(flagIdx)

    if (!isFirst1 && !isFirst2) {
      val sku = buffer2.getString(idIdx)
      val bundle = buffer2.getString(bundleIdx)
      val message = s"Duplicated sku in bundle data: $sku -> $bundle"
      buffer1(msgIdx) = message :: buffer1.getList[String](msgIdx).asScala.toList
    } else if (!isFirst2) {
      buffer1(idIdx) = buffer2.getAs[String](idIdx)
      buffer1(nameIdx) = buffer2.getAs[String](nameIdx)
      buffer1(descIdx) = buffer2.getAs[String](descIdx)
      buffer1(bundleIdx) = buffer2.getAs[String](bundleIdx)
      buffer1(flagIdx) = false
    }
    buffer1(msgIdx) = (buffer1.getList[String](msgIdx).asScala ++ buffer2.getList[String](msgIdx).asScala).toList
  }

  override def evaluate(buffer: Row): Any = {
    Row.fromSeq(Seq(
      buffer.get(nameIdx),
      buffer.get(descIdx),
      buffer.get(bundleIdx),
      buffer.get(msgIdx)
    ))
  }
}
