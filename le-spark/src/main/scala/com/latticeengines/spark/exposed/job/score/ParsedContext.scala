package com.latticeengines.spark.exposed.job.score

import com.latticeengines.domain.exposed.serviceflows.scoring.spark.CalculateExpectedRevenuePercentileJobConfig
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.CalculateExpectedRevenuePercentileJobConfig.ScoreDerivationType
import com.latticeengines.domain.exposed.scoring.ScoreResultField
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation
import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MMap}

class ParsedContext {
    var PREFIX_TEMP_COL = "__TEMP__"
    var standardScoreField = ScoreResultField.Percentile.displayName
    var expectedRevenueField = ScoreResultField.ExpectedRevenue.displayName
    var probabilityField = ScoreResultField.Probability.displayName
    var predictedRevenueField = ScoreResultField.PredictedRevenue.displayName

    var minPct = 5
    var maxPct = 99
    var inputTableName: String = _
    var percentileFieldName: String = _ 
    var modelGuidFieldName: String = _
    var originalScoreFieldMap: Map[String, String] = _
    var fitFunctionParametersMap: Map[String, String] = _
    var outputPercentileFieldName: String = _
    var outputExpRevFieldName: String = _
    var backupPredictedRevFieldName: String = _
    var backupProbabilityFieldName: String = _
    var scoreCountFieldName: String = _
    var targetScoreDerivation: Boolean = _
    var targetScoreDerivationInputs: Map[String, String] = _
    var targetScoreDerivationOutputs: MMap[String, String] = _

    var normalizationRatioMap: Map[String, java.lang.Double] = _
    var scoreDerivationMaps: Map[String, java.util.Map[ScoreDerivationType, ScoreDerivation]] = _
}

object ParsedContext {
  
  def apply(config : CalculateExpectedRevenuePercentileJobConfig): ParsedContext = {
      val context = new ParsedContext
      context.inputTableName = config.inputTableName
      context.percentileFieldName = config.percentileFieldName
      context.modelGuidFieldName = config.modelGuidField
      context.originalScoreFieldMap = if (config.originalScoreFieldMap == null) Map() else config.originalScoreFieldMap.asScala.toMap
      context.fitFunctionParametersMap = if (config.fitFunctionParametersMap == null) Map() else config.fitFunctionParametersMap.asScala.toMap
      context.minPct = config.percentileLowerBound
      context.maxPct = config.percentileUpperBound
      context.normalizationRatioMap = if (config.normalizationRatioMap == null) Map() else config.normalizationRatioMap.asScala.toMap
      context.scoreDerivationMaps = if (config.scoreDerivationMaps == null) Map() else config.scoreDerivationMaps.asScala.toMap 
      context.targetScoreDerivation = config.targetScoreDerivation
      context.targetScoreDerivationInputs = if (config.targetScoreDerivationInputs == null) Map() else config.targetScoreDerivationInputs.asScala.toMap
      context.targetScoreDerivationOutputs = MMap()
      
      val timestamp:Long = System.currentTimeMillis
      context.outputPercentileFieldName = "%sper_%d".format(context.PREFIX_TEMP_COL, timestamp)
      context.outputExpRevFieldName = "%sev_%d".format(context.PREFIX_TEMP_COL, timestamp)
      context.scoreCountFieldName = "%scount_%d".format(context.PREFIX_TEMP_COL, timestamp)
      context.backupPredictedRevFieldName = "__%s_%s".format(context.predictedRevenueField, "raw")
      context.backupProbabilityFieldName = "__%s_%s".format(context.probabilityField, "raw")
      context
  }
}
