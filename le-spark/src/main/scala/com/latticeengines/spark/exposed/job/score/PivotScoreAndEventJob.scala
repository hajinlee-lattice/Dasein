package com.latticeengines.spark.exposed.job.score

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.PivotScoreAndEventJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.domain.exposed.scoring.ScoreResultField
import com.latticeengines.domain.exposed.cdl.scoring.CalculatePositiveEventsFunction
import com.latticeengines.domain.exposed.util.BucketedScoreSummaryUtils.{BUCKET_AVG_SCORE, BUCKET_LIFT, BUCKET_SUM};
import com.latticeengines.domain.exposed.util.BucketedScoreSummaryUtils.{BUCKET_TOTAL_EVENTS, BUCKET_TOTAL_POSITIVE_EVENTS};
import com.latticeengines.domain.exposed.util.BucketedScoreSummaryUtils.{MODEL_AVG, MODEL_GUID, MODEL_SUM};

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions.{when, udf, col, lit, count, avg, sum, asc}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class PivotScoreAndEventJob extends AbstractSparkJob[PivotScoreAndEventJobConfig] {
    val MODEL_GUID = ScoreResultField.ModelId.displayName

    override def runJob(spark: SparkSession, lattice: LatticeContext[PivotScoreAndEventJobConfig]): Unit = {

      val scoreResult: DataFrame = lattice.input.head
      val config: PivotScoreAndEventJobConfig = lattice.config
      
      val avgScoresMap = config.avgScores.asScala.toMap
      val scoreFieldMap = config.scoreFieldMap.asScala.toMap
      val scoreDerivationMap = config.scoreDerivationMap.asScala.toMap
      val fitFunctionParametersMap = config.fitFunctionParametersMap.asScala.toMap
      
      val nodes : scala.collection.mutable.Map[String, DataFrame] = splitNodes(scoreResult, scoreFieldMap)
      var outputs = new ListBuffer[DataFrame]()
      for ((modelGuid, node) <- nodes) {
          val scoreField = scoreFieldMap.getOrElse(modelGuid, InterfaceName.RawScore.name)
          val useEvent = InterfaceName.Event.name == scoreField
          val isEV = InterfaceName.ExpectedRevenue.name == scoreField

          val total = getTotal(node, modelGuid, scoreField, isEV, useEvent)
          val aggregatedNode = aggregate(node, scoreField, isEV, useEvent)
          val scoreDerivation = scoreDerivationMap.getOrElse(modelGuid, null)
          val fitFunctionParams = fitFunctionParametersMap.getOrElse(modelGuid, null)

          val avgScore = avgScoresMap.get(modelGuid)
          val output = createLift(aggregatedNode, total, avgScore, scoreDerivation, fitFunctionParams, isEV, useEvent)
          outputs += output
      }
      val result = outputs.reduce(_ union _)
    
      lattice.output = result::Nil
    }
    
    def aggregate(input : DataFrame, scoreField : String, isEV : Boolean, useEvent : Boolean) : DataFrame = {
        var node = input
        val percentileScoreField = ScoreResultField.Percentile.displayName
        if (useEvent) {
            node = node.withColumn("IsPositiveEvent", when(col(scoreField) === lit(true), lit(1)).otherwise(lit(0)))
            node.groupBy(percentileScoreField, ScoreResultField.ModelId.displayName)
              .agg(
                  count(percentileScoreField).alias(BUCKET_TOTAL_EVENTS), 
                  sum(col("IsPositiveEvent") * 1.0).alias(BUCKET_TOTAL_POSITIVE_EVENTS)).sort(asc(percentileScoreField))
        } else {
             node.groupBy(percentileScoreField, ScoreResultField.ModelId.displayName)
              .agg(
                  count(percentileScoreField).alias(BUCKET_TOTAL_EVENTS),
                  avg(scoreField).alias(BUCKET_AVG_SCORE),
                  sum(scoreField).alias(BUCKET_SUM)).sort(asc(percentileScoreField))
        }
    }
    
    def getTotal(input : DataFrame, modelGuid : String, origScoreField : String, isEV : Boolean, useEvent : Boolean) : DataFrame = {
        var node = input
        var scoreField = origScoreField
        if (useEvent) {
            node = node.withColumn("EventScore", when(col(scoreField) === lit(true), lit(1.0)).otherwise(lit(0.0)))
            scoreField = "EventScore"
        }
        if (isEV) {
            node.select(col(scoreField), col(MODEL_GUID))
              .groupBy(col(MODEL_GUID))
              .agg(avg(scoreField).alias(MODEL_AVG), sum(scoreField).alias(MODEL_SUM))
        } else {
            node.select(col(scoreField), col(MODEL_GUID))
              .groupBy(col(MODEL_GUID))
              .agg(avg(scoreField).alias(MODEL_AVG))
        }
    }

    
    def splitNodes(scoreResult : DataFrame, scoreFieldMap : Map[String, String]) : scala.collection.mutable.Map[String, DataFrame] = {
        val nodes = scala.collection.mutable.Map[String, DataFrame]()
        for ((modelGuid, scoreField) <- scoreFieldMap) {
            var model = scoreResult.filter(col(MODEL_GUID) === modelGuid)
            nodes(modelGuid) = model
        }
        nodes
    }
    
     def createLift(aggregatedNode : DataFrame, total : DataFrame, avgScore : Option[java.lang.Double], scoreDerivation : String, fitFunctionParams : String,
            isEV : Boolean, useEvent : Boolean) : DataFrame = {
        if (useEvent) {
            val modelAvgProbability : Double = avgScore.get
            var aggrNode = aggregatedNode.withColumn("ConversionRate", when(col(BUCKET_TOTAL_EVENTS) === 0, lit(0.0))
                .otherwise(col(BUCKET_TOTAL_POSITIVE_EVENTS) / col(BUCKET_TOTAL_EVENTS) * 1.0))
            aggrNode.withColumn(BUCKET_LIFT, col("ConversionRate") / modelAvgProbability)
        } else {
            var aggrNode = aggregatedNode.join(total, Seq(MODEL_GUID), joinType = "inner")
            aggrNode = aggrNode.withColumn(BUCKET_LIFT, when(col(MODEL_AVG) > 0, col(BUCKET_AVG_SCORE) / col(MODEL_AVG) * 1.0)
                .otherwise(lit(0.0)))
            if (!isEV) {
                aggrNode = getTotalPositiveEvents(aggrNode, scoreDerivation, fitFunctionParams, isEV)
                aggrNode = aggrNode.withColumn(BUCKET_AVG_SCORE, lit(null).cast(DoubleType))
                aggrNode = aggrNode.withColumn(BUCKET_SUM, lit(null).cast(DoubleType))
            } else {
                aggrNode = aggrNode.withColumn(BUCKET_TOTAL_POSITIVE_EVENTS, when(col(BUCKET_TOTAL_EVENTS) === 0, 
                    lit(0.0)).otherwise(col(BUCKET_TOTAL_EVENTS) * col(BUCKET_SUM) / col(MODEL_SUM) * 1.0))
            }
            aggrNode.select(col(ScoreResultField.ModelId.displayName), col(ScoreResultField.Percentile.displayName),
                    col(BUCKET_TOTAL_POSITIVE_EVENTS), col(BUCKET_TOTAL_EVENTS), col(BUCKET_LIFT),
                    col(BUCKET_AVG_SCORE), col(BUCKET_SUM))
        }
    }

    def getTotalPositiveEvents(aggregatedNode : DataFrame, scoreDerivation : String, fitFunctionParams : String,
            isEV : Boolean) : DataFrame = {
        if (scoreDerivation == null || fitFunctionParams == null) {
            getTotalPositiveEventsUsingAvgScore(aggregatedNode)
        } else {
            getTotalPositiveEventsUsingFitFunction(aggregatedNode, scoreDerivation, fitFunctionParams, isEV)
        }
    }

    def getTotalPositiveEventsUsingFitFunction(aggregatedNode : DataFrame, scoreDerivation : String,
            fitFunctionParams : String, isEV : Boolean) : DataFrame = {
        val calculatePositiveEventsFunc = new CalculatePositiveEventsFunction(scoreDerivation, fitFunctionParams, isEV) 
        val positiveEventsUdf = udf((avgScore : Double, totalEvents : Long) => {
          calculatePositiveEventsFunc.calculate(avgScore, totalEvents)
        })
        aggregatedNode.withColumn(BUCKET_TOTAL_POSITIVE_EVENTS,
            positiveEventsUdf(col(BUCKET_AVG_SCORE), col(BUCKET_TOTAL_EVENTS)))
    }

    def getTotalPositiveEventsUsingAvgScore(aggregatedNode : DataFrame) : DataFrame = {
        aggregatedNode.withColumn(BUCKET_TOTAL_POSITIVE_EVENTS, when(col(BUCKET_TOTAL_EVENTS) === 0, lit(0.0))
            .otherwise(col(BUCKET_AVG_SCORE) * col(BUCKET_TOTAL_EVENTS) * 1.0))
    }
}
