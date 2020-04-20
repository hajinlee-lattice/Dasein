package com.latticeengines.spark.aggregation

import com.latticeengines.domain.exposed.metadata.InterfaceName
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

private[spark] class DedupHierarchyProductAggregation extends UserDefinedAggregateFunction {

  private val Id = InterfaceName.Id.name
  private val Name = InterfaceName.ProductName.name
  private val Description = InterfaceName.Description.name
  private val Line = InterfaceName.ProductLine.name
  private val Family = InterfaceName.ProductFamily.name
  private val Category = InterfaceName.ProductCategory.name
  private val Messages = "Messages"

  private val idIdx = 0
  private val nameIdx = 1
  private val descIdx = 2
  private val lineIdx = 3
  private val famIdx = 4
  private val catIdx = 5
  private val msgIdx = 6
  private val flagIdx = 7

  override def inputSchema: StructType = StructType(List(
    StructField(Id, StringType),
    StructField(Name, StringType),
    StructField(Description, StringType),
    StructField(Line, StringType),
    StructField(Family, StringType),
    StructField(Category, StringType)
  ))

  override def bufferSchema: StructType = StructType(List(
    StructField(Id, StringType),
    StructField(Name, StringType),
    StructField(Description, StringType),
    StructField(Line, StringType),
    StructField(Family, StringType),
    StructField(Category, StringType),
    StructField(Messages, ArrayType(StringType)),
    StructField("first", BooleanType)
  ))

  override def dataType: DataType = StructType(List(
    StructField(Name, StringType),
    StructField(Description, StringType),
    StructField(Line, StringType),
    StructField(Family, StringType),
    StructField(Category, StringType),
    StructField(Messages, ArrayType(StringType))
  ))

  override def deterministic: Boolean = false

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = null
    buffer(1) = null
    buffer(2) = null
    buffer(3) = null
    buffer(4) = null
    buffer(5) = null
    buffer(6) = List()
    buffer(7) = true
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val isFirst = buffer.getBoolean(flagIdx)

    if (isFirst) {
      copyToBuffer(buffer, input)
    } else {
      val sku = input.getString(idIdx)
      val message = s"Duplicated sku in hierarchy data: $sku"
      buffer(msgIdx) = message :: buffer.getList[String](msgIdx).asScala.toList
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val isFirst1 = buffer1.getBoolean(flagIdx)
    val isFirst2 = buffer2.getBoolean(flagIdx)

    if (!isFirst1 && !isFirst2) {
      val sku = buffer2.getString(idIdx)
      val message = s"Duplicated sku in hierarchy data: $sku"
      buffer1(msgIdx) = message :: buffer1.getList[String](msgIdx).asScala.toList
    } else if (!isFirst2) {
      copyToBuffer(buffer1, buffer2)
    }
    buffer1(msgIdx) = (buffer1.getList[String](msgIdx).asScala ++ buffer2.getList[String](msgIdx).asScala).toList
  }

  override def evaluate(buffer: Row): Any = {
    Row.fromSeq(Seq(
      buffer.get(nameIdx),
      buffer.get(descIdx),
      buffer.get(lineIdx),
      buffer.get(famIdx),
      buffer.get(catIdx),
      buffer.get(msgIdx)
    ))
  }

  private def copyToBuffer(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(idIdx) = input.getString(idIdx)
    buffer(nameIdx) = input.getString(nameIdx)
    buffer(descIdx) = input.getString(descIdx)
    buffer(lineIdx) = input.getString(lineIdx)
    buffer(famIdx) = input.getString(famIdx)
    buffer(catIdx) = input.getString(catIdx)
    buffer(flagIdx) = false
  }

}
