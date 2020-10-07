package com.latticeengines.spark.exposed.job.score

import org.apache.commons.collections4.MapUtils

import com.latticeengines.domain.exposed.cdl.scoring.LookupPercentileForRevenueFunction2
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.CalculateExpectedRevenuePercentileJobConfig.ScoreDerivationType
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions.{col, udf, lit}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

object PercentileLookupEvHelper {

    def calculate(context: ParsedContext, calculatePercentile: DataFrame): DataFrame = {
        lookupPercentileForEV(context, calculatePercentile)
    }

    def lookupPercentileForEV(context: ParsedContext, nodeInput: DataFrame): DataFrame = {
      // Use "ev" field from evScoreDerivation.json to lookup for
      var node = nodeInput
      if (!context.originalScoreFieldMap.isEmpty) {
        val retainedFields = node.columns
        node = lookupPercentileFromScoreDerivation(context.scoreDerivationMaps, context.originalScoreFieldMap,
                context.modelGuidFieldName, context.outputPercentileFieldName, context.expectedRevenueField,
                context.minPct, context.maxPct, node)
        node = node.select(retainedFields map col: _*)
      }
      node
    }

    def lookupPercentileFromScoreDerivation (
            scoreDerivationMap: Map[String,  java.util.Map[ScoreDerivationType, ScoreDerivation]], //
            originalScoreFieldMap: Map[String, String], //
            modelGuidFieldName: String, percentileFieldName: String, revenueFieldName: String, minPct: Integer, maxPct: Integer,
            mergedScoreCount: DataFrame): DataFrame = {

        var merged: DataFrame = null
        val mergedCache = mergedScoreCount.persist(StorageLevel.DISK_ONLY)
        for ((modelGuid, scoreField) <- originalScoreFieldMap) {
          var node = mergedCache.filter(col(modelGuidFieldName) === modelGuid) 
          if (scoreDerivationMap.contains(modelGuid)) {
              val calFunc = new LookupPercentileForRevenueFunction2(scoreDerivationMap(modelGuid).get(ScoreDerivationType.EV))
              val calFuncUdf = udf((revenue : Double) => {
                calFunc.calculate(revenue)
              })
              node = node.withColumn(percentileFieldName, calFuncUdf(col(revenueFieldName)))
          }
          if (merged == null) {
              merged = node
          } else {
              merged = merged.union(node)
          }
        }
        merged
    }
}
