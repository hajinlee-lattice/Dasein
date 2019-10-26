package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit
import com.latticeengines.domain.exposed.spark.cdl.CalculateDeltaJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.{Row, SparkSession}


class CalculateDeltaJob extends AbstractSparkJob[CalculateDeltaJobConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[CalculateDeltaJobConfig]): Unit = {
    val config: CalculateDeltaJobConfig = lattice.config
    val newData = loadHdfsUnit(spark, config.getNewData.asInstanceOf[HdfsDataUnit])
    val oldData = if (config.getOldData != null) loadHdfsUnit(spark, config.getOldData.asInstanceOf[HdfsDataUnit]) else spark.createDataFrame(spark.sparkContext.emptyRDD[Row], newData.schema)
    val newDFAlias = "newDfAlias"
    val oldDFAlias = "oldDFAlias"

    val positiveDelta = newData.alias(newDFAlias).join(oldData.alias(oldDFAlias), Seq(config.getJoinKey), "leftanti") //
      // Conditionally filter out records where the joinKey is null in the newData
      .transform { df =>
      if (config.isFilterJoinKeyNulls) {
        df.where(newData.col(config.getJoinKey).isNotNull)
      }
      else {
        df
      }
    }
      .select(newDFAlias + ".*")

    val negativeDelta = oldData.alias(oldDFAlias).join(newData.alias(newDFAlias), Seq(config.getJoinKey), "leftanti") //
      // Conditionally filter out records where the joinKey is null in the oldData
      .transform { df =>
      if (config.isFilterJoinKeyNulls) {
        df.where(oldData.col(config.getJoinKey).isNotNull)
      }
      else {
        df
      }
    }
      .select(oldDFAlias + ".*")

    lattice.output = List(positiveDelta, negativeDelta)
  }

}
