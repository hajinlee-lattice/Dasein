package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit
import com.latticeengines.domain.exposed.spark.cdl.CalculateDeltaJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.functions.{col, concat, lit, when}
import org.apache.spark.sql.{Row, SparkSession}


class CalculateDeltaJob extends AbstractSparkJob[CalculateDeltaJobConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[CalculateDeltaJobConfig]): Unit = {
    val config: CalculateDeltaJobConfig = lattice.config
    val newData = loadHdfsUnit(spark, config.getNewData.asInstanceOf[HdfsDataUnit])
    val oldData = if (config.getOldData != null) loadHdfsUnit(spark, config.getOldData.asInstanceOf[HdfsDataUnit]) else spark.createDataFrame(spark.sparkContext.emptyRDD[Row], newData.schema)
    val newDFAlias = "newDfAlias"
    val oldDFAlias = "oldDFAlias"
    val compositeKey = "account_contact"

    val positiveDelta = if (config.getSecondaryJoinKey != null && !config.getFilterPrimaryJoinKeyNulls) {
      newData.withColumn(compositeKey, concat(col(config.getSecondaryJoinKey), lit("_"), when(col(config.getPrimaryJoinKey).isNotNull, col(config.getPrimaryJoinKey)).otherwise(lit("null")))).alias(newDFAlias)
        .join(oldData.withColumn(compositeKey, concat(col(config.getSecondaryJoinKey), lit("_"), when(col(config.getPrimaryJoinKey).isNotNull, col(config.getPrimaryJoinKey)).otherwise(lit("null")))).alias(oldDFAlias), Seq(compositeKey), "leftanti")
        .select(config.getPrimaryJoinKey, config.getSecondaryJoinKey)
    }
    else {
      newData.alias(newDFAlias).join(oldData.alias(oldDFAlias), Seq(config.getPrimaryJoinKey), "leftanti") //
    }
      // Conditionally filter out records where the joinKey is null in the newData
      .transform { df =>
      if (config.getFilterPrimaryJoinKeyNulls) {
        df.where(newData.col(config.getPrimaryJoinKey).isNotNull)
      }
      else {
        df
      }
    }
      .select(newDFAlias + ".*")

    val negativeDelta = if (config.getSecondaryJoinKey != null && !config.getFilterPrimaryJoinKeyNulls) {
      oldData.withColumn(compositeKey, concat(col(config.getSecondaryJoinKey), lit("_"), when(col(config.getPrimaryJoinKey).isNotNull, col(config.getPrimaryJoinKey)).otherwise(lit("null")))).alias(oldDFAlias)
        .join(newData.withColumn(compositeKey, concat(col(config.getSecondaryJoinKey), lit("_"), when(col(config.getPrimaryJoinKey).isNotNull, col(config.getPrimaryJoinKey)).otherwise(lit("null")))).alias(newDFAlias), Seq(compositeKey), "leftanti")
        .select(config.getPrimaryJoinKey, config.getSecondaryJoinKey)
    }
    else {
      oldData.alias(oldDFAlias).join(newData.alias(newDFAlias), Seq(config.getPrimaryJoinKey), "leftanti") //
    }
      // Conditionally filter out records where the joinKey is null in the oldData
      .transform { df =>
      if (config.getFilterPrimaryJoinKeyNulls) {
        df.where(oldData.col(config.getPrimaryJoinKey).isNotNull)
      }
      else {
        df
      }
    }
      .select(oldDFAlias + ".*")

    lattice.output = List(positiveDelta, negativeDelta)
  }

}
