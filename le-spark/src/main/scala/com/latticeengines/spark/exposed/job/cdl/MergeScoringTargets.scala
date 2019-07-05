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
    val isRuleBased = config.isRuleBased

    val expandedDfs = (inputDfs zip containers) map { t =>
      val df = t._1
      val container = t._2
      val modelId = container.getModel.getId
      if (isRuleBased) {
        expand(df, modelId, None)
      } else {
        val modelGuid = Some(container.getModel.asInstanceOf[AIModel].getModelSummaryId)
        expand(df, modelId, modelGuid)
      }
    }

    val merged = expandedDfs.reduce((d1, d2) => {
      MergeUtils.concat2(d1, d2)
    })

    // finish
    lattice.output = merged :: Nil
  }

  private def expand(df: DataFrame, modelId: String, modelGuid: Option[String]): DataFrame = {
    val expanded = df
      .withColumn("__Composite_Key__", concat(col("AccountId"), lit("_" + modelId)))
      .withColumn("ModelId", lit(modelId))

    val expandModelGuid = modelGuid match {
      case Some(guid) => expanded.withColumn("Model_GUID", lit(guid))
      case _ => expanded
    }

    val withPeriodId = modelGuid match {
      case Some(_) =>
        if (df.columns.contains("PeriodId")) {
          expandModelGuid.withColumn("PeriodId", col("PeriodId").cast("long"))
        } else {
          expandModelGuid.withColumn("PeriodId", lit(0L).cast("long"))
        }
      case _ => expandModelGuid
    }

    val selections: Seq[String] = modelGuid match {
      case Some(_) => List("__Composite_Key__", "AccountId", "PeriodId", "ModelId", "Model_GUID")
      case _ => List("__Composite_Key__", "AccountId", "ModelId", "Rating")
    }

    withPeriodId.select(selections map col: _*)
  }

}
