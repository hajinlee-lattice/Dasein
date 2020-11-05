package com.latticeengines.spark.exposed.job.cdl

import java.text.SimpleDateFormat
import java.util.TimeZone

import com.latticeengines.domain.exposed.cdl.GenerateRecommendationCSVContext
import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit
import com.latticeengines.domain.exposed.serviceflows.cdl.DeltaCampaignLaunchWorkflowConfiguration
import com.latticeengines.domain.exposed.spark.cdl.GenerateRecommendationCSVConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.CSVUtils
import org.apache.commons.collections4.MapUtils
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

class GenerateRecommendationCSVJob extends AbstractSparkJob[GenerateRecommendationCSVConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[GenerateRecommendationCSVConfig]): Unit = {
    val config: GenerateRecommendationCSVConfig = lattice.config
    val generateRecommendationCSVContext: GenerateRecommendationCSVContext = config.getGenerateRecommendationCSVContext
    val fields: Seq[String] = generateRecommendationCSVContext.getFields.asScala
    var finalDfs: List[DataFrame] = List()
    finalDfs = lattice.input.map(csvDf => {
      val columnsNotExist: Seq[String] = fields.filter(field => !csvDf.columns.contains(field))
      var finalCSVDf = csvDf
      if (generateRecommendationCSVContext.isIgnoreAccountsWithoutContacts) {
        finalCSVDf = finalCSVDf.filter(col(DeltaCampaignLaunchWorkflowConfiguration.CONTACT_ATTR_PREFIX + InterfaceName.ContactId.name()).isNotNull)
      }
      for (colName <- columnsNotExist) {
        finalCSVDf = finalCSVDf.withColumn(colName, lit(null).cast(StringType))
      }
      finalCSVDf = finalCSVDf.select(fields.map(name => col(name)): _*)
      finalCSVDf = changeToDisplayName(finalCSVDf, generateRecommendationCSVContext)
      if (generateRecommendationCSVContext.getAddExportTimestamp) {
        val tz = TimeZone.getTimeZone("UTC")
        val now = System.currentTimeMillis
        val fmtr = new SimpleDateFormat(generateRecommendationCSVContext.getDateFormat);
        fmtr.setTimeZone(tz)
        val fmtrUdf = udf((ts: Long) => fmtr.format(ts))
        finalCSVDf.withColumn(InterfaceName.LatticeExportTime.name(), fmtrUdf(lit(now)))
      }
      finalCSVDf
    })
    lattice.output = finalDfs
  }

  private def changeToDisplayName(input: DataFrame, generateRecommendationCSVContext: GenerateRecommendationCSVContext): DataFrame = {
    if (MapUtils.isEmpty(generateRecommendationCSVContext.getDisplayNames)) {
      input
    } else {
      val attrsToRename: Map[String, String] = generateRecommendationCSVContext.getDisplayNames.asScala.toMap
        .filterKeys(input.columns.contains(_))
      val newAttrs = input.columns.map(c => attrsToRename.getOrElse(c, c))
      input.toDF(newAttrs: _*)
    }
  }

  override def finalizeJob(spark: SparkSession, latticeCtx: LatticeContext[GenerateRecommendationCSVConfig]): List[HdfsDataUnit] = {
    CSVUtils.dfToCSV(spark, false, latticeCtx.targets, latticeCtx.output)
  }
}