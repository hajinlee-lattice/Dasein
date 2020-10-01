package com.latticeengines.spark.exposed.job.score

import com.latticeengines.domain.exposed.cdl.scoring.CalculatePercentile2
import com.latticeengines.domain.exposed.scoring.ScoreResultField
import org.apache.spark.sql.{ DataFrame }
import scala.collection.JavaConverters._
import org.apache.spark.sql.functions.{ col, udf, lit, desc, row_number, min, max }
import org.apache.spark.sql.types._
import scala.collection.mutable.{ Map => MMap }
import org.apache.spark.sql.expressions.Window
import scala.collection.mutable.ListBuffer
import org.apache.spark.storage.StorageLevel

object PercentileCalculationHelper {
  val INCREASE_ID = "__increasing_id"
  val MIN_COL = "__min_col"
  val MAX_COL = "__max_col"
  def calculate(context: ParsedContext, mergedScoreCount: DataFrame, trySecondarySort: Boolean,
    targetScoreDerivation: Boolean): DataFrame = {
    val mergedCache = mergedScoreCount.persist(StorageLevel.DISK_ONLY)
    val secondarySortFieldName = if (trySecondarySort) context.outputExpRevFieldName else null
    var merged: DataFrame = null
    for ((modelGuid, scoreField) <- context.originalScoreFieldMap) {
      val originalScoreFieldName = getScoreFieldName(context.originalScoreFieldMap, modelGuid)
      val node = mergedCache.filter(col(context.modelGuidFieldName) === modelGuid)
      val model = getModel(context, node, secondarySortFieldName, originalScoreFieldName, context.targetScoreDerivation, modelGuid: String)
      if (merged == null) {
        merged = model
      } else {
        merged = merged.union(model)
      }
    }
    merged
  }

  def getScoreFieldName(originalScoreFieldMap: Map[String, String], modelGuid: String): String = {
    var scoreField = originalScoreFieldMap.getOrElse(modelGuid, ScoreResultField.RawScore.displayName)
    val isNotRevenue: Boolean = ScoreResultField.RawScore.displayName == scoreField
    if (isNotRevenue) {
      ScoreResultField.RawScore.displayName
    } else {
      val scoreField2 = originalScoreFieldMap.get(modelGuid).get
      val predicted: Boolean = ScoreResultField.PredictedRevenue.displayName == scoreField2
      if (predicted) {
        ScoreResultField.PredictedRevenue.displayName
      } else {
        ScoreResultField.ExpectedRevenue.displayName
      }
    }
  }

  def getModel(context: ParsedContext, node: DataFrame, secondarySortFieldName: String,
        originalScoreFieldName: String, targetScoreDerivation: Boolean, modelGuid: String): DataFrame = {
    calculatePercentileByFieldName(context.modelGuidFieldName, context.scoreCountFieldName,
      originalScoreFieldName, context.percentileFieldName, context.standardScoreField, secondarySortFieldName,
      context.minPct, context.maxPct, node, context.targetScoreDerivationInputs, context.targetScoreDerivationOutputs,
      targetScoreDerivation, modelGuid)
  }

  def calculatePercentileByFieldName(modelGuidFieldName: String, scoreCountFieldName: String,
    originalScoreFieldName: String, percentileFieldName: String, defaultPercentileFieldName: String,
    secondarySortFieldName: String, minPct: Integer, maxPct: Integer, nodeInput: DataFrame,
    targetScoreDerivationInputs: Map[String, String],
    targetScoreDerivationOutputs: MMap[String, String],
    targetScoreDerivation: Boolean, modelGuid: String): DataFrame = {

    if (ScoreResultField.RawScore.displayName == originalScoreFieldName) {
      return nodeInput
    }

    var node = nodeInput.withColumn(percentileFieldName, lit(null).cast(IntegerType))
    val returnedFields = List(node.columns)
    val sortFieldList = if (secondarySortFieldName == null || secondarySortFieldName == originalScoreFieldName) {
      List(originalScoreFieldName)
    } else {
      List(originalScoreFieldName, secondarySortFieldName)
    }

    val windowSpec = Window.partitionBy(modelGuidFieldName)
      .orderBy(sortFieldList map desc: _*)
    node = node.withColumn(INCREASE_ID, row_number().over(windowSpec))
    val calFunc = new CalculatePercentile2(minPct, maxPct,
        targetScoreDerivation,
        targetScoreDerivationInputs.get(modelGuid).orNull)
    val calFuncUdf = udf((rawScore: Double, totalCount: Long, increaseId: Long) => {
      calFunc.calculate(rawScore, totalCount, increaseId)
    })
    node = node.withColumn(percentileFieldName, calFuncUdf(
      col(originalScoreFieldName),
      col(scoreCountFieldName),
      col(INCREASE_ID) - 1))

    if (targetScoreDerivation && !targetScoreDerivationInputs.contains(modelGuid)) {
      val derivation = node.groupBy(percentileFieldName)
        .agg(min(col(originalScoreFieldName)).as(MIN_COL), max(col(originalScoreFieldName)).as(MAX_COL))
        .select(Seq(percentileFieldName, MIN_COL, MAX_COL) map col: _*)

      val derivationMap = new java.util.HashMap[Integer, java.util.List[java.lang.Double]]()
      derivation.collect().foreach(row => {
        val minMax = new java.util.ArrayList[java.lang.Double]
        val pct = row.getInt(0)
        val minV = row.getDouble(1)
        val maxV = row.getDouble(2)
        minMax.add(minV)
        minMax.add(maxV)
        derivationMap.put(pct, minMax)
      })
      
      val targetScoreDerivationStr = calFunc.getTargetScoreDerivationValue(derivationMap)
      if (targetScoreDerivationStr != null) {
        println("----- BEGIN SCRIPT OUTPUT -----")
        println(s"modelGuid=${modelGuid}, targetScoreDerivationStr=${targetScoreDerivationStr}")
        println("----- END SCRIPT OUTPUT -----")
        targetScoreDerivationOutputs(modelGuid) = targetScoreDerivationStr
      }
    }

    node.drop(INCREASE_ID)
  }
}
