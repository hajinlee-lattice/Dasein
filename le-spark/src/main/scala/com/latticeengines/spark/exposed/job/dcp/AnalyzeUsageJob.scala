package com.latticeengines.spark.exposed.job.dcp

import com.latticeengines.domain.exposed.datacloud.`match`.VboUsageConstants
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit
import com.latticeengines.domain.exposed.spark.dcp.AnalyzeUsageConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.CSVUtils
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

class AnalyzeUsageJob extends AbstractSparkJob[AnalyzeUsageConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[AnalyzeUsageConfig]): Unit = {
    val config: AnalyzeUsageConfig = lattice.config
    val input: DataFrame = lattice.input.head

    val outputFields: List[String] = config.getOutputFields.asScala.toList

    val rawFields: List[String] = config.getRawOutputMap.values().asScala.toList

    val renamed = selectAndRename(input, config)

    var outputWithAllFields: DataFrame = null
    outputWithAllFields = renamed

    if (outputFields.nonEmpty) {
      for (field <- outputFields.diff(outputWithAllFields.columns)) {
        if (field == VboUsageConstants.ATTR_DRT) {
          outputWithAllFields = outputWithAllFields.withColumn(field, lit(config.getDRTAttr).cast(StringType))
        } else if (field == VboUsageConstants.ATTR_SUBSCRIBER_COUNTRY) {
          outputWithAllFields = outputWithAllFields.withColumn(field, lit(config.getSubscriberCountry).cast(StringType))
        } else if (field == VboUsageConstants.ATTR_SUBSCRIBER_NAME) {
          outputWithAllFields = outputWithAllFields.withColumn(field, lit(config.getSubscriberName).cast(StringType))
        } else if (field == VboUsageConstants.ATTR_SUBSCRIBER_NUMBER) {
          outputWithAllFields = outputWithAllFields.withColumn(field, lit(config.getSubscriberNumber).cast(StringType))
        } else if (field == VboUsageConstants.ATTR_LEID) {
          outputWithAllFields = outputWithAllFields.withColumn(field, lit(config.getUploadId).cast(StringType))
        }  else if (field == VboUsageConstants.ATTR_DELIVERY_CHANNEL) {
          outputWithAllFields = outputWithAllFields.withColumn(field, lit("Web Application").cast(StringType))
        }  else if (field == VboUsageConstants.ATTR_DELIVERY_MODE) {
          outputWithAllFields = outputWithAllFields.withColumn(field, lit("Transactional Batch").cast(StringType))
        }  else if (field == VboUsageConstants.ATTR_APPID) {
          outputWithAllFields = outputWithAllFields.withColumn(field, lit("157").cast(StringType))
        }  else if (field == VboUsageConstants.ATTR_CAPPID) {
          outputWithAllFields = outputWithAllFields.withColumn(field, lit("46").cast(StringType))
        } else if (!rawFields.contains(field)) {
          outputWithAllFields = outputWithAllFields.withColumn(field, lit("").cast(StringType))
        }
      }
    }

    outputWithAllFields = outputWithAllFields.select(outputFields map col: _*)

    lattice.output = outputWithAllFields :: Nil
  }

  private def selectAndRename(input: DataFrame, config: AnalyzeUsageConfig): DataFrame = {
    val attrNames = config.getRawOutputMap.asScala.toMap
    val selected = input.columns.filter(attrNames.keySet)
    val filtered = input.select(selected map col: _*)
    val newNames = filtered.columns.map(c => attrNames.getOrElse(c, c))
    filtered.toDF(newNames: _*)
  }

  override def finalizeJob(spark: SparkSession, latticeCtx: LatticeContext[AnalyzeUsageConfig]): List[HdfsDataUnit] = {
    CSVUtils.dfToCSVWithCoalesceLimit(spark, false, 1000000,  latticeCtx.targets, latticeCtx.output)
  }

}
