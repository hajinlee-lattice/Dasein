package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.spark.cdl.JourneyStageJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.SparkSession

class GenerateJourneyStageJob extends AbstractSparkJob[JourneyStageJobConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[JourneyStageJobConfig]): Unit = {
    val config = lattice.config
    // TODO add real implementation
    lattice.output = lattice.input(config.masterAccountTimeLineIdx) :: lattice.input(config.diffAccountTimeLineIdx) :: Nil
  }
}
