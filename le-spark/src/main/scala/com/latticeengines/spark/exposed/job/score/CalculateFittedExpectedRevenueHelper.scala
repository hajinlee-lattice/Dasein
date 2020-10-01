package com.latticeengines.spark.exposed.job.score

import org.apache.commons.collections4.MapUtils
import com.latticeengines.domain.exposed.cdl.scoring.CalculateFittedExpectedRevenueFunction2
import com.latticeengines.domain.exposed.scoring.ScoreResultField
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.CalculateExpectedRevenuePercentileJobConfig.ScoreDerivationType
import org.apache.spark.sql.{DataFrame}
import scala.collection.JavaConverters._
import org.apache.spark.sql.functions.{col, udf, lit}
import org.apache.spark.sql.types._

object CalculateFittedExpectedRevenueHelper {
  
    def calculate(context: ParsedContext, calculatePercentile: DataFrame, retainedFields: List[String]): DataFrame = {
        var merged: DataFrame = null
        for ((modelGuid, scoreField) <- context.originalScoreFieldMap) {
          var node = calculatePercentile.filter(col(context.modelGuidFieldName) === modelGuid) 
          val evFitFunctionParamsStr = context.fitFunctionParametersMap.get(modelGuid)
          val normalizationRatio = context.normalizationRatioMap.get(modelGuid)
          val avgProbabilityTestDataset = getAvgProbabilityTestDataset(context, modelGuid)
          if (ScoreResultField.ExpectedRevenue.displayName == scoreField) {
            val calFunc = new CalculateFittedExpectedRevenueFunction2(
                                  normalizationRatio.orNull,
                                  avgProbabilityTestDataset,
                                  evFitFunctionParamsStr.orNull)
            
            val calFuncUdf = udf((percentile: Integer, probability: Double) => {
                  calFunc.calculate(percentile, probability)
            })
            node = node.withColumn("CalResult", calFuncUdf(col(context.outputPercentileFieldName), col(context.probabilityField)))
                .withColumn(context.probabilityField, col("CalResult")(0))
                .withColumn(context.expectedRevenueField, col("CalResult")(1)).drop("CalResult")
          }
          if (merged == null) {
              merged = node
          } else {
              merged = merged.union(node)
          }
        }
        merged
    return merged.drop(context.standardScoreField) //
      .withColumnRenamed(
        context.outputPercentileFieldName, //
        context.standardScoreField) //
      .select(retainedFields map col: _*)
    }

    def getAvgProbabilityTestDataset(context: ParsedContext, modelGuid: String): Double = {
        var avgProbabilityTestDataset:Double = 0
        if (!context.scoreDerivationMaps.isEmpty
                && MapUtils.isNotEmpty(context.scoreDerivationMaps.get(modelGuid).orNull)
                && context.scoreDerivationMaps.get(modelGuid).get.get(ScoreDerivationType.PROBABILITY) != null) {
            avgProbabilityTestDataset = context.scoreDerivationMaps.get(modelGuid).get
                    .get(ScoreDerivationType.PROBABILITY).averageProbability
        }
        return avgProbabilityTestDataset
    }
}
