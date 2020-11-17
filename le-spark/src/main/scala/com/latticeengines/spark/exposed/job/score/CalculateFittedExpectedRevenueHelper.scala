package com.latticeengines.spark.exposed.job.score

import com.latticeengines.domain.exposed.cdl.scoring.CalculateFittedExpectedRevenueFunction2
import com.latticeengines.domain.exposed.scoring.ScoreResultField
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.CalculateExpectedRevenuePercentileJobConfig.ScoreDerivationType
import org.apache.spark.sql.{DataFrame}
import scala.collection.JavaConverters._
import org.apache.spark.sql.functions.{col, udf, lit}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

object CalculateFittedExpectedRevenueHelper {
  
    def calculate(context: ParsedContext, calculatePercentile: DataFrame, retainedFields: List[String]): DataFrame = {
        
        val (model, predictedModel, evModel) = NodeJobSplitter.splitEv(calculatePercentile, context.originalScoreFieldMap,
            context.modelGuidFieldName)
        val evModelCache = if (evModel == null) evModel else evModel.persist(StorageLevel.DISK_ONLY)
        var mergedEv: DataFrame = null
        for ((modelGuid, scoreField) <- context.originalScoreFieldMap) {
          if (ScoreResultField.ExpectedRevenue.displayName == scoreField) {
            var node = evModelCache.filter(col(context.modelGuidFieldName) === modelGuid) 
            val evFitFunctionParamsStr = context.fitFunctionParametersMap.get(modelGuid)
            val normalizationRatio = context.normalizationRatioMap.get(modelGuid)
            val avgProbabilityTestDataset = getAvgProbabilityTestDataset(context, modelGuid)
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
            if (mergedEv == null) {
                mergedEv = node
            } else {
                mergedEv = mergedEv.union(node)
            }
          }
        }
        
        val models = List(model, predictedModel, mergedEv).filter(e => e != null)
        var merged = models(0)
        for (i <- 1 until models.length) {
            merged = merged.union(models(i))
        }
        return merged.drop(context.standardScoreField) //
          .withColumnRenamed(
            context.outputPercentileFieldName, //
            context.standardScoreField) //
          .select(retainedFields map col: _*)
    }

    def getAvgProbabilityTestDataset(context: ParsedContext, modelGuid: String): Double = {
        var avgProbabilityTestDataset:Double = 0
        if (!context.scoreDerivationMaps.isEmpty
                && context.scoreDerivationMaps.contains(modelGuid)
                && context.scoreDerivationMaps(modelGuid).get(ScoreDerivationType.PROBABILITY) != null) {
            avgProbabilityTestDataset = context.scoreDerivationMaps(modelGuid)
                    .get(ScoreDerivationType.PROBABILITY).averageProbability
             println("----- BEGIN SCRIPT OUTPUT -----")
             println(s"modelGuid=${modelGuid}, avgProbabilityTestDataset=${avgProbabilityTestDataset}")
             println("----- END SCRIPT OUTPUT -----")
        }
        return avgProbabilityTestDataset
    }
}
