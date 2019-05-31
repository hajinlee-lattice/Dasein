package com.latticeengines.spark.exposed.job.common

import java.text.SimpleDateFormat
import java.util.TimeZone

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit
import com.latticeengines.domain.exposed.spark.common.ConvertToCSVConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.commons.collections4.MapUtils
import org.apache.spark.sql.functions.{col, udf, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._

class ConvertToCSVJob extends AbstractSparkJob[ConvertToCSVConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[ConvertToCSVConfig]): Unit = {
    val config: ConvertToCSVConfig = lattice.config
    val input = lattice.input.head

    val formatted = formatDateAttrs(input, config)
    val renamed = changeToDisplayName(formatted, config)

    val result = renamed
    lattice.output = result :: Nil
  }

  private def formatDateAttrs(input: DataFrame, config: ConvertToCSVConfig): DataFrame = {
    if (MapUtils.isEmpty(config.getDateAttrsFmt)) {
      input
    } else {
      val attrsToFmt: Map[String, String] = config.getDateAttrsFmt.asScala.toMap
        .filterKeys(input.columns.contains(_))
      val suffix = scala.util.Random.nextString(6)
      val tz =
        if (config.getTimeZone == null)
        TimeZone.getTimeZone("UTC")
      else
        TimeZone.getTimeZone(config.getTimeZone)
      val originalCols = input.columns.clone
      input.columns.foldLeft(input)((df, c) => {
        if (attrsToFmt.contains(c)) {
          val fmtr = new SimpleDateFormat(attrsToFmt(c))
          fmtr.setTimeZone(tz)
          val fmtrUdf = udf((ts: Long) => fmtr.format(ts))
          val tempCol = c + "_str_" + suffix
          df.withColumn(c, when(col(c).isNotNull, fmtrUdf(col(c))))
        } else {
          df
        }
      })
    }
  }

  private def changeToDisplayName(input: DataFrame, config: ConvertToCSVConfig): DataFrame = {
    if (MapUtils.isEmpty(config.getDisplayNames)) {
      input
    } else {
      val attrsToRename: Map[String, String] = config.getDisplayNames.asScala.toMap
        .filterKeys(input.columns.contains(_))
      val newAttrs = input.columns.map(c => attrsToRename.getOrElse(c, c))
      input.toDF(newAttrs: _*)
    }
  }

  private def saveToCsv(df: DataFrame, path: String, compress: Boolean): Unit = {
    var writer = df.coalesce(1).write
      .format("csv")
      .option("header", value = true)
      .option("quote", "\"")
      .option("escape", "\"")
    if (compress) {
      writer = writer.option("compression", "gzip")
    }
    writer.save(path)
  }

  override def finalizeJob(spark: SparkSession, latticeCtx: LatticeContext[ConvertToCSVConfig]): List[HdfsDataUnit] = {
    val config: ConvertToCSVConfig = latticeCtx.config
    val compress: Boolean = if (config.getCompress == null) false else config.getCompress

    val targets: List[HdfsDataUnit] = latticeCtx.targets
    val output: List[DataFrame] = latticeCtx.output
    if (targets.length != output.length) {
      throw new IllegalArgumentException(s"${targets.length} targets are declared " //
        + s"but ${output.length} outputs are generated!")
    }
    targets.zip(output).map { t =>
      val tgt = t._1
      val df = t._2.persist(StorageLevel.DISK_ONLY).toDF()
      saveToCsv(df, tgt.getPath, compress=compress)
      tgt.setCount(df.count())
      df.unpersist(blocking = false)
      tgt
    }
  }

}
