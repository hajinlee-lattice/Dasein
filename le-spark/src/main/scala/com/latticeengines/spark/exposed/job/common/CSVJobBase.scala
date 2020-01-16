package com.latticeengines.spark.exposed.job.common

import java.text.SimpleDateFormat
import java.util.TimeZone

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit
import com.latticeengines.domain.exposed.spark.common.CSVJobConfigBase
import com.latticeengines.spark.exposed.job.AbstractSparkJob
import org.apache.commons.collections4.MapUtils
import org.apache.spark.sql.functions.{col, udf, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._
import scala.util.Success

abstract class CSVJobBase[C <: CSVJobConfigBase] extends AbstractSparkJob[C] {

  def saveToCsv(df: DataFrame, path: String, compress: Boolean): Unit = {
    var writer = df.coalesce(1).write
      .option("header", value = true)
      .option("quote", "\"")
      .option("escape", "\"")
    if (compress) {
      writer = writer.option("compression", "gzip")
    }
    writer.csv(path)
  }

  def dfToCSV(spark: SparkSession, config: C, targets: List[HdfsDataUnit], output: List[DataFrame]): List[HdfsDataUnit] = {
    if (targets.length != output.length) {
      throw new IllegalArgumentException(s"${targets.length} targets are declared " //
        + s"but ${output.length} outputs are generated!")
    }
    val compress: Boolean = if (config.getCompress == null) false else config.getCompress
    targets.zip(output).map { t =>
      val tgt = t._1
      val df = t._2
      saveToCsv(df, tgt.getPath, compress = compress)
      val suffix = if (compress) ".csv.gz" else ".csv"
      var reader = spark.read.format("csv")
        .option("maxColumns", 40000)
        .option("header", "true")
      if (compress) {
        reader = reader.option("compression", "gzip")
      }
      val df2 = reader.load(tgt.getPath + "/*" + suffix)
      tgt.setCount(df2.count())
      tgt
    }
  }

  def formatDateAttrs(input: DataFrame, config: C): DataFrame = {
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
          val fmtrUdf = udf((ts: String) => {
            val result = scala.util.Try(ts.toLong)
            result match {
              case Success(_) => fmtr.format(result.get)
              case _ => ts
            }
          })
          df.withColumn(c, when(col(c).isNotNull, fmtrUdf(col(c))))
        } else {
          df
        }
      })
    }
  }

  def changeToDisplayName(input: DataFrame, config: C): DataFrame = {
    if (MapUtils.isEmpty(config.getDisplayNames)) {
      input
    } else {
      val attrsToRename: Map[String, String] = config.getDisplayNames.asScala.toMap.filterKeys(input.columns.contains(_))
      val newAttrs = input.columns.map(c => attrsToRename.getOrElse(c, c))
      input.toDF(newAttrs: _*)
    }
  }

  def customizeField(input: DataFrame, config: C): DataFrame = {
    val formatted = formatDateAttrs(input, config)
    val renamed = changeToDisplayName(formatted, config)
    renamed
  }
}
