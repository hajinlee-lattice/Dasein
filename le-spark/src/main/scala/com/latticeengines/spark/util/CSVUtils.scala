package com.latticeengines.spark.util

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

private[spark] object CSVUtils {

  def dfToCSV(spark: SparkSession, compress: Boolean, targets: List[HdfsDataUnit], output: List[DataFrame]): List[HdfsDataUnit] = {
    if (targets.length != output.length) {
      throw new IllegalArgumentException(s"${targets.length} targets are declared " //
              + s"but ${output.length} outputs are generated!")
    }
    targets.zip(output).map { t =>
      val tgt = t._1
      var df: DataFrame = t._2
      df = df.persist(StorageLevel.DISK_ONLY)
      saveToCsv(df, tgt.getPath, compress = compress)
      tgt.setCount(df.count())
      tgt
    }
  }

  private def saveToCsv(df: DataFrame, path: String, compress: Boolean): Unit = {
    var writer = df.coalesce(1).write //
            .option("header", value = true) //
            .option("quote", "\"") //
            .option("escape", "\"")
    if (compress) {
      writer = writer.option("compression", "gzip")
    }
    writer.csv(path)
  }
}
