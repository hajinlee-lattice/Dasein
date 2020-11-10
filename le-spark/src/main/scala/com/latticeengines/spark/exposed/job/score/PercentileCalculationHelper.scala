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
import collection.JavaConversions._
import org.apache.spark.storage.StorageLevel

object PercentileCalculationHelper {
  val INCREASE_ID = "__increasing_id"
  val MIN_COL = "__min_col"
  val MAX_COL = "__max_col"
  def calculate(context: ParsedContext, mergedScoreCount: DataFrame, trySecondarySort: Boolean,
    targetScoreDerivation: Boolean): DataFrame = {

    var (model, predictedModel, evModel) = NodeJobSplitter.splitEv(mergedScoreCount, context.originalScoreFieldMap,
            context.modelGuidFieldName)
            
    val secondarySortFieldName = if (trySecondarySort) context.outputExpRevFieldName else null
    model = getModel(context, model, secondarySortFieldName, ScoreResultField.RawScore.displayName,
            targetScoreDerivation)
    predictedModel = getModel(context, predictedModel, secondarySortFieldName,
            ScoreResultField.PredictedRevenue.displayName, targetScoreDerivation);
    evModel = getModel(context, evModel, secondarySortFieldName, ScoreResultField.ExpectedRevenue.displayName,
            targetScoreDerivation);
    
    val models = List(model, predictedModel, evModel).filter(e => e != null)
    var merged = models(0)
    for (i <- 1 until models.length) {
        merged = merged.union(models(i))
    }
    merged
        
  }

  def getModel(context: ParsedContext, node: DataFrame, secondarySortFieldName: String,
        originalScoreFieldName: String, targetScoreDerivation: Boolean): DataFrame = {
    if (node == null) {
      return node;
    }
    calculatePercentileByFieldName(context.modelGuidFieldName, context.scoreCountFieldName,
      originalScoreFieldName, context.percentileFieldName, context.standardScoreField, secondarySortFieldName,
      context.minPct, context.maxPct, node, context.targetScoreDerivationInputs, context.targetScoreDerivationOutputs,
      context.originalScoreFieldMap, targetScoreDerivation, true)
  }

  def calculatePercentileByFieldName(modelGuidFieldName: String, scoreCountFieldName: String,
    originalScoreFieldName: String, percentileFieldName: String, defaultPercentileFieldName: String,
    secondarySortFieldName: String, minPct: Integer, maxPct: Integer, nodeInput: DataFrame,
    targetScoreDerivationInputs: Map[String, String],
    targetScoreDerivationOutputs: MMap[String, String],
    originalScoreFieldMap: Map[String, String],
    targetScoreDerivation: Boolean, skipProbability: Boolean): DataFrame = {

    if ((ScoreResultField.RawScore.displayName == originalScoreFieldName) && skipProbability) {
      return nodeInput
    }

    println("----- BEGIN SCRIPT OUTPUT -----")
    println(s"modelGuid1=${modelGuidFieldName}")
    println("----- END SCRIPT OUTPUT -----")
    
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
        mapAsJavaMap(targetScoreDerivationInputs))
    
    val calFuncUdf = udf((modelId: String, rawScore: Double, totalCount: Long, increaseId: Long) => {
      calFunc.calculate(modelId, rawScore, totalCount, increaseId)
    })
    node = node.withColumn(percentileFieldName, calFuncUdf(
      col(modelGuidFieldName),
      col(originalScoreFieldName),
      col(scoreCountFieldName),
      col(INCREASE_ID) - 1))
      
    if (targetScoreDerivation) {
      val derivations = node.groupBy(modelGuidFieldName, percentileFieldName)
        .agg(min(col(originalScoreFieldName)).as(MIN_COL), max(col(originalScoreFieldName)).as(MAX_COL))
        .select(Seq(modelGuidFieldName, percentileFieldName, MIN_COL, MAX_COL) map col: _*).persist(StorageLevel.DISK_ONLY)
        
      for ((modelGuid, scoreField) <- originalScoreFieldMap) {
        if (!targetScoreDerivationInputs.contains(modelGuid)) {
          println("----- BEGIN SCRIPT OUTPUT -----")
          println(s"modelGuid5=${modelGuidFieldName}")
          println("----- END SCRIPT OUTPUT -----")
          
          val derivationMap = new java.util.HashMap[Integer, java.util.List[java.lang.Double]]()
          var derivation = derivations.filter(col(modelGuidFieldName) === modelGuid)
          derivation.collect().foreach(row => {
            val minMax = new java.util.ArrayList[java.lang.Double]()
            val pct = row.getInt(1)
            val minV = row.getDouble(2)
            val maxV = row.getDouble(3)
            minMax.add(minV)
            minMax.add(maxV)
            derivationMap.put(pct, minMax)
          })
          val targetScoreDerivationStr = calFunc.getTargetScoreDerivationValue(modelGuid, derivationMap)
          if (targetScoreDerivationStr != null) {
            targetScoreDerivationOutputs(modelGuid) = targetScoreDerivationStr
          }
        }
      }
    }
    node.drop(INCREASE_ID)
  }
}
