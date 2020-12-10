package com.latticeengines.spark.util

import com.latticeengines.domain.exposed.metadata.InterfaceName
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, struct, udf}
import org.apache.spark.sql.types.StringType

private[spark] object TransactionUtils {

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
}
