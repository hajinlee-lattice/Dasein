package com.latticeengines.spark.exposed.job.common

import java.text.SimpleDateFormat
import java.util.TimeZone

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit
import com.latticeengines.domain.exposed.spark.common.ConvertToCSVConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.{CSVUtils, DisplayNameUtils}
import org.apache.commons.collections4.MapUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions.{col, lit, udf, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

class ConvertToCSVJob extends AbstractSparkJob[ConvertToCSVConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[ConvertToCSVConfig]): Unit = {
    val config: ConvertToCSVConfig = lattice.config
    val input = lattice.input.head

    val withTimestamp = addTimestamp(input, config)
    val formatted = formatDateAttrs(withTimestamp, config)
    val renamed = DisplayNameUtils.changeToDisplayName(formatted, config.getDisplayNames)

    val result = renamed
    lattice.output = result :: Nil
  }

  private def addTimestamp(input: DataFrame, config: ConvertToCSVConfig): DataFrame = {
    val timeAttr = config.getExportTimeAttr
    if (StringUtils.isNotBlank(timeAttr)) {
      val now = System.currentTimeMillis
      input.withColumn(timeAttr, lit(now))
    } else {
      input
    }
  }

  private def formatDateAttrs(input: DataFrame, config: ConvertToCSVConfig): DataFrame = {
    if (MapUtils.isEmpty(config.getDateAttrsFmt)) {
      input
    } else {
      val attrsToFmt: Map[String, String] = config.getDateAttrsFmt.asScala.toMap
        .filterKeys(input.columns.contains(_))
      val tz =
        if (config.getTimeZone == null)
          TimeZone.getTimeZone("UTC")
        else
          TimeZone.getTimeZone(config.getTimeZone)
      input.columns.foldLeft(input)((df, c) => {
        if (attrsToFmt.contains(c)) {
          val fmtr = new SimpleDateFormat(attrsToFmt(c))
          fmtr.setTimeZone(tz)
          val fmtrUdf = udf((ts: Long) => fmtr.format(ts))
          df.withColumn(c, when(col(c).isNotNull, fmtrUdf(col(c))))
        } else {
          df
        }
      })
    }
  }

  override def finalizeJob(spark: SparkSession, latticeCtx: LatticeContext[ConvertToCSVConfig]): List[HdfsDataUnit] = {
    val config: ConvertToCSVConfig = latticeCtx.config
    val compress: Boolean = if (config.getCompress == null) false else config.getCompress
    CSVUtils.dfToCSV(spark, compress, latticeCtx.targets, latticeCtx.output)
  }

}
