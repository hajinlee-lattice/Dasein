package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.pls.RatingEngine
import com.latticeengines.domain.exposed.pls.RatingEngine.ScoreType
import com.latticeengines.domain.exposed.spark.cdl.PivotRatingsConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.MergeUtils
import org.apache.spark.sql.functions.{col, first, lit, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

class PivotRatings extends AbstractSparkJob[PivotRatingsConfig] {

  private val AccountId = InterfaceName.AccountId.name
  private val ModelId = InterfaceName.ModelId.name
  private val CreateTime = InterfaceName.CDLCreatedTime.name
  private val UpdateTime = InterfaceName.CDLUpdatedTime.name
  private val EngineId = "EngineId"
  private val Rating = InterfaceName.Rating.name
  private val Score = RatingEngine.ScoreType.Score.name
  private val ExpectedRevenue = RatingEngine.ScoreType.ExpectedRevenue.name
  private val PredictedRevenue = RatingEngine.ScoreType.PredictedRevenue.name

  override def runJob(spark: SparkSession, lattice: LatticeContext[PivotRatingsConfig]): Unit = {
    val config: PivotRatingsConfig = lattice.config
    val inputs = lattice.input
    val aiIdx = if (config.getAiSourceIdx == null) -1 else config.getAiSourceIdx.asInstanceOf[Int]
    val ruleIdx = if (config.getRuleSourceIdx == null) -1 else config.getRuleSourceIdx.asInstanceOf[Int]
    val inactiveIdx = if (config.getInactiveSourceIdx == null) -1 else config.getInactiveSourceIdx.asInstanceOf[Int]
    val idAttrsMap = config.getIdAttrsMap.asScala.toMap // model id to engine id map
    val aiModelIds = if (config.getAiModelIds == null) List() else config.getAiModelIds.asScala.toList
    val inactiveEngineIds = //
      if (config.getInactiveEngineIds == null) List() else config.getInactiveEngineIds.asScala.toList

    val rulePivoted: Option[DataFrame] =
      if (ruleIdx < 0) {
        None
      } else {
        val ruleIdAttrs = idAttrsMap.filterKeys(modelId => !aiModelIds.contains(modelId)).map(identity)
        Some(pivotRuleBased(inputs(ruleIdx), ruleIdAttrs))
      }

    val aiPivoted: Option[DataFrame] =
      if (aiIdx < 0) {
        None
      } else {
        val AIIdAttrs = idAttrsMap.filterKeys(modelId => aiModelIds.contains(modelId)).map(identity)
        val evModelIds: List[String] = if (config.getEvModelIds == null) List() else config.getEvModelIds.asScala.toList
        Some(pivotAI(inputs(aiIdx), AIIdAttrs, evModelIds))
      }

    val newRatings: Option[DataFrame] = ((aiPivoted, rulePivoted) match {
      case (Some(df), None) => Some(df)
      case (None, Some(df)) => Some(df)
      case (Some(df1), Some(df2)) => Some(df1.join(df2, Seq(AccountId), "outer"))
      case _ => None
    }) match {
      case Some(df) =>
        val currentTime = System.currentTimeMillis
        Some(df.withColumn(CreateTime, lit(currentTime)).withColumn(UpdateTime, lit(currentTime)))
      case _ => None
    }

    val oldRatings: Option[DataFrame] =
      if (inactiveEngineIds.isEmpty || inactiveIdx < 0) {
        None
      } else {
        extractInactiveRatings(inputs(inactiveIdx), inactiveEngineIds)
      }

    val result: DataFrame = (newRatings, oldRatings) match {
      case (Some(df), None) => df
      case (None, Some(df)) => df
      case (Some(df1), Some(df2)) => //
        MergeUtils.merge2(df2, df1, Seq(AccountId), Set(CreateTime), overwriteByNull = true)
      case _ => throw new IllegalStateException("Neither new ratings nor old ratings exist.")
    }

    // finish
    lattice.output = result::Nil
  }

  private def pivotRuleBased(raw: DataFrame, idAttrsMap: Map[String, String]): DataFrame = {
    pivotScore(renameModelIdToEngineId(raw, idAttrsMap), idAttrsMap.values.toSeq, Rating, "")
  }

  private def pivotAI(raw: DataFrame, idAttrsMap: Map[String, String], evModelIds: List[String]): DataFrame = {
    val renamed = renameModelIdToEngineId(raw, idAttrsMap)
    val rating = pivotScore(renamed, idAttrsMap.values.toSeq, Rating, "")

    // pivot score
    val scoreSuffix = RatingEngine.SCORE_ATTR_SUFFIX.get(ScoreType.Score)
    val scorePivoted = pivotScore(renamed, idAttrsMap.values.toSeq, Score, scoreSuffix)

    val ratingAndScore = rating.join(scorePivoted, Seq(AccountId), joinType = "left")

    if (evModelIds.nonEmpty) {
      val evEngineIds = idAttrsMap.filterKeys(modelId => evModelIds.contains(modelId)).values.toSeq
      // pivot ExpectedRevenue
      val erSuffix = RatingEngine.SCORE_ATTR_SUFFIX.get(ScoreType.ExpectedRevenue)
      val erPivoted = pivotScore(renamed, evEngineIds, ExpectedRevenue, erSuffix)
      // pivot PredictedRevenue
      val prSuffix = RatingEngine.SCORE_ATTR_SUFFIX.get(ScoreType.PredictedRevenue)
      val prPivoted = pivotScore(renamed, evEngineIds, PredictedRevenue, prSuffix)
      // join
      val evPivoted = erPivoted.join(prPivoted, Seq(AccountId), joinType = "outer")
      ratingAndScore.join(evPivoted, Seq(AccountId), joinType = "left")
    } else {
      ratingAndScore
    }
  }

  private def renameModelIdToEngineId(df: DataFrame, idAttrsMap: Map[String, String]): DataFrame = {
    val renameIdFunc: String => String = modelId => idAttrsMap.getOrElse(modelId, null)
    val renameIdUdf = udf(renameIdFunc)
    df //
      .filter(col(AccountId).isNotNull) //
      .withColumn(EngineId, renameIdUdf(col(ModelId))) //
      .drop(ModelId)
  }

  private def pivotScore(raw: DataFrame, engineIds: Seq[String], scoreCol: String, suffix: String): DataFrame = {
    val pivoted = raw //
      .select(AccountId, EngineId, scoreCol) //
      .filter(col(scoreCol).isNotNull) //
      .groupBy(AccountId) //
      .pivot(EngineId, engineIds) //
      .agg(first(scoreCol))
    if (suffix != "") {
      val attrsMap = engineIds.map(engineId => (engineId, engineId + "_" + suffix)).toMap
      pivoted.toDF(pivoted.columns map (c => attrsMap.getOrElse(c, c)): _*)
    } else {
      pivoted
    }
  }

  private def extractInactiveRatings(df: DataFrame, engineIds: Seq[String]): Option[DataFrame] = {
    val inactiveRatingAttrs = engineIds.flatMap(eid => {
      eid :: RatingEngine.SCORE_ATTR_SUFFIX.values.asScala.map(suffix => eid + "_" + suffix).toList
    }).toList intersect df.columns
    if (inactiveRatingAttrs.isEmpty) {
      None
    } else {
      val inactiveAttrs: List[String] = AccountId :: CreateTime :: UpdateTime :: inactiveRatingAttrs
      val selected = df.select(inactiveAttrs map col: _*)
      Some(selected.filter(row => {
        // keep accounts with at least one not-null rating
        row.getValuesMap(inactiveRatingAttrs).values.exists(v => v.asInstanceOf[Any] != null)
      }))
    }
  }

}
