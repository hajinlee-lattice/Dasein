package com.latticeengines.spark.util

import com.latticeengines.domain.exposed.cdl.activity.StreamAttributeDeriver
import com.latticeengines.domain.exposed.cdl.activity.StreamAttributeDeriver.Calculation
import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.query.BusinessEntity
import org.apache.spark.sql.DataFrame

private[spark] object DeriveAttrsUtils {

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

  def getEntityIdColumnName(entity: BusinessEntity): String = {
    entity match {
      case BusinessEntity.Account => InterfaceName.AccountId.name
      case BusinessEntity.Contact => InterfaceName.ContactId.name
      case _ => throw new UnsupportedOperationException(s"Entity $entity is not supported")
    }
  }
}
