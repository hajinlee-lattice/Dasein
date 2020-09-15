package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.cdl.GenerateRecommendationCSVContext
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit
import com.latticeengines.domain.exposed.spark.cdl.GenerateRecommendationCSVConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.CSVUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

class GenerateRecommendationCSVJob extends AbstractSparkJob[GenerateRecommendationCSVConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[GenerateRecommendationCSVConfig]): Unit = {
    val config: GenerateRecommendationCSVConfig = lattice.config
    val generateRecommendationCSVContext: GenerateRecommendationCSVContext = config.getGenerateRecommendationCSVContext
    var finalDfs: List[DataFrame] = List()
    finalDfs = lattice.input.map(csvDf => csvDf)
    lattice.output = finalDfs
  }

  override def finalizeJob(spark: SparkSession, latticeCtx: LatticeContext[GenerateRecommendationCSVConfig]): List[HdfsDataUnit] = {
    CSVUtils.dfToCSV(spark, false, latticeCtx.targets, latticeCtx.output)
  }
}
