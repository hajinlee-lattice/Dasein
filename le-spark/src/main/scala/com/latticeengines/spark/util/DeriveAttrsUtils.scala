package com.latticeengines.spark.util

import com.latticeengines.common.exposed.util.AvroUtils
import com.latticeengines.domain.exposed.cdl.activity.ActivityRowReducer.Operator
import com.latticeengines.domain.exposed.cdl.activity.StreamAttributeDeriver.Calculation
import com.latticeengines.domain.exposed.cdl.activity._
import com.latticeengines.domain.exposed.metadata.InterfaceName.{AccountId, ContactId}
import com.latticeengines.domain.exposed.query.BusinessEntity
import org.apache.avro.Schema.Type
import org.apache.commons.collections4.CollectionUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, DoubleType, LongType, StringType}
import org.apache.spark.sql.{Column, DataFrame, Row}

import scala.collection.JavaConversions._

private[spark] object DeriveAttrsUtils {

  val PARTITION_COL_PREFIX: String = "PK_"

  val TIME_REDUCER_OPERATIONS: Seq[Operator] = Seq(Operator.Earliest, Operator.Latest)

  val VERSION_COL: String = "versionStamp"

  def getAggr(df: DataFrame, deriver: StreamAttributeDeriver): Column = {
    val calculation: Calculation = deriver.getCalculation
    calculation match {
      case Calculation.SUM => getSum(df, deriver)
      case Calculation.MAX => getMax(df, deriver)
      case Calculation.MIN => getMin(df, deriver)
      case Calculation.TRUE => getTrue(df, deriver)
      case _ => throw new UnsupportedOperationException(s"$calculation is not implemented")
    }
  }

  def applyReducer(df: DataFrame, reducer: ActivityRowReducer): DataFrame = {
    if (isTimeReducingOperation(reducer.getOperator)) {
      applyTimeActivityRowReducer(df, reducer)
    } else {
      throw new UnsupportedOperationException(s"${reducer.getOperator} is not implemented")
    }
  }

  def categorizeValues(df: DataFrame, config: CategorizeValConfig, entityIdCol: String): DataFrame = {
    config match {
      case doubleValuesConfig: CategorizeDoubleConfig => categorizeDoubleValues(df, doubleValuesConfig, entityIdCol)
      case _ => throw new UnsupportedOperationException("config type not supported")
    }
  }

  def categorizeDoubleValues(df: DataFrame, config: CategorizeDoubleConfig, entityIdCol: String): DataFrame = {
    val mappingUdf = UserDefinedFunction((value: Double) => config.findCategory(value), StringType, Some(Seq(DoubleType)))
    var cloned = df.as("cloned")
    df.columns.foreach(colName => {
      if (!colName.equals(entityIdCol))
        cloned = cloned.withColumn(colName, mappingUdf(df(colName)))
    })
    cloned
  }

  def checkSingleSource(deriver: StreamAttributeDeriver): Unit = {
    if (CollectionUtils.isEmpty(deriver.getSourceAttributes) && deriver.getSourceAttributes.size != 1) {
      throw new UnsupportedOperationException("Aggregation takes exactly 1 source attributes")
    }
  }

  def getSum(df: DataFrame, deriver: StreamAttributeDeriver): Column = {
    checkSingleSource(deriver)
    sum(df(deriver.getSourceAttributes.get(0))).alias(deriver.getTargetAttribute)
  }

  def getMax(df: DataFrame, deriver: StreamAttributeDeriver): Column = {
    checkSingleSource(deriver)
    max(df(deriver.getSourceAttributes.get(0))).alias(deriver.getTargetAttribute)
  }

  def getMin(df: DataFrame, deriver: StreamAttributeDeriver): Column = {
    checkSingleSource(deriver)
    min(df(deriver.getSourceAttributes.get(0))).alias(deriver.getTargetAttribute)
  }

  def getTrue(df: DataFrame, deriver: StreamAttributeDeriver): Column = {
    lit(true).cast(BooleanType).as(deriver.getTargetAttribute)
  }

  def applyTimeActivityRowReducer(df: DataFrame, reducer: ActivityRowReducer): DataFrame = {
    val fields: Seq[String] = reducer.getGroupByFields.toSeq
    val operator = reducer.getOperator
    val argument: String = reducer.getArguments.head

    df.groupBy(fields.head, fields: _*).agg(
      (operator match {
        case Operator.Earliest => min(argument)
        case Operator.Latest => max(argument)
        case _ => throw new UnsupportedOperationException(s"$operator is not supported for row reducing")
      }).as(argument)
    ).join(df, fields :+ argument, "inner")
  }

  def getEntityIdColumnNameFromEntity(entity: BusinessEntity): String = {
    entity match {
      case BusinessEntity.Account => AccountId.name
      case BusinessEntity.Contact => ContactId.name
      case _ => throw new UnsupportedOperationException(s"Entity $entity is not supported")
    }
  }

  def getEntityIdColsFromStream(stream: AtlasStream): Seq[String] = {
    if (stream.getAggrEntities.contains(BusinessEntity.Contact.name)) {
      Seq(AccountId.name, ContactId.name)
    } else {
      Seq(AccountId.name)
    }
  }

  def appendPartitionColumns(df: DataFrame, colsPartition: Seq[String]): DataFrame = {
    var newDF: DataFrame = df
    colsPartition.foreach((colName: String) => newDF = newDF.withColumn(PARTITION_COL_PREFIX + colName, newDF(colName)))
    newDF
  }

  def dropPartitionColumns(df: DataFrame): DataFrame = {
    var newDF: DataFrame = df
    df.columns.foreach((colName: String) => if (colName.startsWith(PARTITION_COL_PREFIX)) newDF = newDF.drop(colName))
    newDF
  }

  def fillZero(df: DataFrame, javaClass: String): DataFrame = {
    val colType: Type = AvroUtils.getAvroType(javaClass)
    colType match {
      case Type.LONG => df.na.fill(0: Long)
      case _ => throw new UnsupportedOperationException(s"${colType.toString} is not supported to fill zero")
    }
  }

  def fillFalse(df: DataFrame, javaClass: String): DataFrame = {
    val colType: Type = AvroUtils.getAvroType(javaClass)
    colType match {
      case Type.BOOLEAN => df.na.fill(false)
      case _ => throw new UnsupportedOperationException(s"${colType.toString} is not supported to fill false")
    }
  }

  def appendNullColumn(df: DataFrame, attrName: String, javaClass: String): DataFrame = {
    val colType: Type = AvroUtils.getAvroType(javaClass)
    colType match {
      case Type.LONG => df.withColumn(attrName, lit(null).cast(LongType))
      case Type.BOOLEAN => df.withColumn(attrName, lit(null).cast(BooleanType))
      case Type.STRING => df.withColumn(attrName, lit(null).cast(StringType))
      case _ => throw new UnsupportedOperationException(s"${colType.toString} is not supported for appending null columns")
    }
  }

  def isTimeReducingOperation(operator: Operator): Boolean = {
    TIME_REDUCER_OPERATIONS.contains(operator)
  }

  def appendVersionStamp(df: DataFrame, version: Long): DataFrame = {
    df.withColumn(VERSION_COL, lit(version).cast(LongType))
  }

  def concatColumns: UserDefinedFunction = udf((row: Row) => row.mkString("_"))

  def getCompositeColumn(colNames: Seq[String]): Column = {
    concatColumns(struct(colNames.map(colName => trim(col(colName))): _*))
  }
}
