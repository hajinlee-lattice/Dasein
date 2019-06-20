package com.latticeengines.spark.exposed.job.common

import com.latticeengines.domain.exposed.spark.common.SparkCountRecordsConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

class SparkCountRecordsJob extends AbstractSparkJob[SparkCountRecordsConfig]{

  override def runJob(spark: SparkSession, lattice: LatticeContext[SparkCountRecordsConfig]): Unit = {
    val paths: Array[String] = lattice.config.globs
    var count = 0L
    var dfs: List[DataFrame] = List[DataFrame]()

    if (!verifyFormat(".avro", paths)) throw new Exception("Only avro files are compatible with this job")
    paths.map(path => {
      var df = spark.read.format("avro").load(path)
      count += df.count
      dfs = dfs :+ df
    })
    lattice.output = dfs
    lattice.outputStr = count.toString
  }

  def verifyFormat(expected: String, paths: Array[String]): Boolean = {
    var valid = true
    paths.map(path => valid = valid && path.endsWith(expected))
    valid
  }
}
