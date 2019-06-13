package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.pls.{AIModel, RatingModelContainer}
import com.latticeengines.domain.exposed.spark.cdl.MergeScoringTargetsConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.MergeUtils
import org.apache.spark.sql.functions.{col, concat, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

class MergeScoringTargets extends AbstractSparkJob[MergeScoringTargetsConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[MergeScoringTargetsConfig]): Unit = {
    val config: MergeScoringTargetsConfig = lattice.config
    val inputDfs = lattice.input
    val containers: List[RatingModelContainer] = config.getContainers.asScala.toList

    val expandedDfs = (inputDfs zip containers) map { t =>
      val df = t._1
      val container = t._2
      val modelId = container.getModel.getId
      val modelGuid = container.getModel.asInstanceOf[AIModel].getModelSummaryId
      expand(df, modelId, modelGuid)
    }

    val merged = expandedDfs.reduce((d1, d2) => {
      MergeUtils.concat2(d1, d2)
    })

    // finish
    lattice.output = merged :: Nil
  }

  def expand(df: DataFrame, modelId: String, modelGuid: String): DataFrame = {
    val currentTime = System.currentTimeMillis()
    val expanded = df
      .withColumn("__Composite_Key__", concat(col("AccountId"), lit("_" + modelId)))
      .withColumn("ModelId", lit(modelId))
      .withColumn("Model_GUID", lit(modelGuid))
      .withColumn("CDLUpdatedTime", lit(currentTime))

    val withPeriodId =
      if (df.columns.contains("PeriodId")) {
        expanded.withColumn("PeriodId", col("PeriodId").cast("long"))
      } else {
        expanded.withColumn("PeriodId", lit(0L).cast("long"))
      }

    withPeriodId.select( //
      col("__Composite_Key__"), //
      col("AccountId"), //
      col("PeriodId"), //
      col("ModelId"), //
      col("Model_GUID"), //
      col("CDLUpdatedTime") //
    )
  }

}
