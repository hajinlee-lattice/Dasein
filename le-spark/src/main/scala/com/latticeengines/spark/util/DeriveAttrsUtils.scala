package com.latticeengines.spark.util

import com.latticeengines.domain.exposed.cdl.activity.StreamAttributeDeriver.Calculation
import com.latticeengines.domain.exposed.cdl.activity.{AtlasStream, StreamAttributeDeriver}
import com.latticeengines.domain.exposed.metadata.InterfaceName.{AccountId, ContactId}
import com.latticeengines.domain.exposed.query.BusinessEntity
import org.apache.spark.sql.DataFrame

private[spark] object DeriveAttrsUtils {

  val PARTITION_COL_PREFIX: String = "PK_"

  def getDeriverFunction(deriver: StreamAttributeDeriver): (DataFrame, Seq[String], String, Boolean, String) => DataFrame = {
    val calculation: Calculation = deriver.getCalculation
    calculation match {
      case Calculation.COUNT => count
      case Calculation.SUM => sum
      case Calculation.MAX => max
      case Calculation.MIN => min
    }
  }

  // sum, max, and min only accept single input attribute
  def count(df: DataFrame, sourceAttributes: Seq[String], targetAttribute: String, shouldRenameCol: Boolean, colRename: String): DataFrame = {
    df.groupBy(sourceAttributes.head, sourceAttributes.tail: _*).count()
    // TODO - need to rename column since count(something) cannot be written to avro
  }

  def sum(df: DataFrame, sourceAttributes: Seq[String], targetAttribute: String, shouldRenameCol: Boolean, colRename: String): DataFrame = {
    val res: DataFrame = df.groupBy(sourceAttributes.head, sourceAttributes.tail: _*).sum(targetAttribute)
    if (shouldRenameCol) {
      res.withColumnRenamed(s"sum($targetAttribute)", colRename)
    } else {
      res
    }
  }

  def max(df: DataFrame, sourceAttributes: Seq[String], targetAttribute: String, shouldRenameCol: Boolean, colRename: String): DataFrame = {
    val res: DataFrame = df.groupBy(sourceAttributes.head, sourceAttributes.tail: _*).max(sourceAttributes.head)
    if (shouldRenameCol) {
      res.withColumnRenamed(s"max($targetAttribute)", targetAttribute)
    } else {
      res
    }
  }

  def min(df: DataFrame, sourceAttributes: Seq[String], targetAttribute: String, shouldRenameCol: Boolean, colRename: String): DataFrame = {
    val res: DataFrame = df.groupBy(sourceAttributes.head, sourceAttributes.tail: _*).min(sourceAttributes.head)
    if (shouldRenameCol) {
      res.withColumnRenamed(s"min($targetAttribute)", targetAttribute)
    } else {
      res
    }
  }

  def getMetricsGroupEntityIdColumnName(entity: BusinessEntity): String = {
    entity match {
      case BusinessEntity.Account => AccountId.name
      case BusinessEntity.Contact => ContactId.name
      case _ => throw new UnsupportedOperationException(s"Entity $entity is not supported")
    }
  }

  def getGroupByEntityColsFromStream(stream: AtlasStream): Seq[String] = {
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
}
