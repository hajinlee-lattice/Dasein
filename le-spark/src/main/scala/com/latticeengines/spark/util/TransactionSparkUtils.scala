package com.latticeengines.spark.util

import com.latticeengines.domain.exposed.metadata.InterfaceName
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, struct, udf}
import org.apache.spark.sql.types.{DoubleType, StringType}

private[spark] object TransactionSparkUtils {

  val DEFAULT_TXN_TYPE = "Purchase"

  def renameFieldsAndAddPeriodName(df: DataFrame, renameMapping: Map[String, String], periodName: String): DataFrame = {
    renameMapping.foldLeft(df) { (df, entry) =>
      val (src, target) = (entry._1, entry._2)
      df.withColumnRenamed(src, target)
        .withColumn(InterfaceName.PeriodName.name, lit(periodName).cast(StringType))
    }
  }

  def generateCompKey(compositeSrc: Seq[String]): Column = {
    val generateCompKey: UserDefinedFunction = udf((row: Row) => row.mkString(""))
    generateCompKey(struct(compositeSrc.map(col): _*))
  }

  def castMetricsColType(df: DataFrame): DataFrame = {
    df.withColumn(InterfaceName.Cost.name, col(InterfaceName.Cost.name).cast(DoubleType))
      .withColumn(InterfaceName.Amount.name, col(InterfaceName.Amount.name).cast(DoubleType))
      .withColumn(InterfaceName.Quantity.name, col(InterfaceName.Quantity.name).cast(DoubleType))
  }
}
