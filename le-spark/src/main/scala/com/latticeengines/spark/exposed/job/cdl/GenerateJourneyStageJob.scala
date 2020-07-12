package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.cdl.activity.JourneyStage
import com.latticeengines.domain.exposed.query.AggregateLookup
import com.latticeengines.domain.exposed.spark.cdl.JourneyStageJobConfig
import com.latticeengines.domain.exposed.spark.common.GroupingUtilConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.GroupingUtil
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

class GenerateJourneyStageJob extends AbstractSparkJob[JourneyStageJobConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[JourneyStageJobConfig]): Unit = {
    val config = lattice.config
    // TODO add real implementation
    lattice.output = lattice.input(config.masterAccountTimeLineIdx) :: lattice.input(config.diffAccountTimeLineIdx) :: Nil
  }

  def getQualifiedAccounts(df: DataFrame, stage: JourneyStage): DataFrame = {
    stage.getPredicates.asScala.map(sp => GroupingUtil.getGroupedDf(df, GroupingUtilConfig.from(stage, sp)) //
      .where(df.col(AggregateLookup.Aggregator.COUNT.name).geq(lit(sp.getNoOfEvents)))) //
      .reduce(_ union _) //
  }
}
