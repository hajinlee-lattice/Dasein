package com.latticeengines.spark.util

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit
import org.apache.spark.sql.{DataFrame, SparkSession}

private[spark] object CSVUtils {

  def dfToCSV(spark: SparkSession, compress: Boolean, targets: List[HdfsDataUnit], output: List[DataFrame]): List[HdfsDataUnit] = {
    if (targets.length != output.length) {
      throw new IllegalArgumentException(s"${targets.length} targets are declared " //
        + s"but ${output.length} outputs are generated!")
    }
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
}
