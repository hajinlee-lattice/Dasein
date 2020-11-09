package com.latticeengines.spark.exposed.job.score

import com.latticeengines.domain.exposed.serviceflows.scoring.spark.RecalculatePercentileScoreJobConfig
import com.latticeengines.spark.exposed.job.{ AbstractSparkJob, LatticeContext }
import com.latticeengines.domain.exposed.cdl.scoring.CalculatePercentile2
import com.latticeengines.domain.exposed.scoring.ScoreResultField
import org.apache.spark.sql.{ DataFrame, SparkSession }
import scala.collection.JavaConverters._
import org.apache.spark.sql.functions.{ col, count, desc }
import org.apache.spark.sql.types._
import scala.collection.mutable.{ Map => MMap }
import org.apache.spark.sql.expressions.Window
import scala.collection.mutable.ListBuffer

class RecalculatePercentileScoreJob extends AbstractSparkJob[RecalculatePercentileScoreJobConfig] {

    val SCORE_COUNT_FIELD_NAME: String = ScoreResultField.RawScore.displayName + "_count"
    override def runJob(spark: SparkSession, lattice: LatticeContext[RecalculatePercentileScoreJobConfig]): Unit = {
      
      val config: RecalculatePercentileScoreJobConfig = lattice.config
      var inputTable: DataFrame = lattice.input.head
    
      val modelGuidFieldName: String = config.modelGuidField
      val rawScoreFieldName: String = config.rawScoreFieldName
      val scoreFieldName: String = config.scoreFieldName
      val minPct: Integer = config.percentileLowerBound
      val maxPct: Integer = config.percentileUpperBound
      val originalScoreFieldMap: Map[String, String] = config.originalScoreFieldMap.asScala.toMap
      val targetScoreDerivationInputs: Map[String, String] = if (config.targetScoreDerivationInputs == null) Map() else config.targetScoreDerivationInputs.asScala.toMap
      var targetScoreDerivationOutputs: MMap[String, String] = MMap()
      val targetScoreDerivation = config.targetScoreDerivation

      var mergedScoreCount: DataFrame = mergeCount(inputTable, modelGuidFieldName)
      val (modelNode, evModelNode) = NodeJobSplitter.splitRevenue(mergedScoreCount, originalScoreFieldMap,
          modelGuidFieldName)
          
      var model: DataFrame = null
      var evModel: DataFrame = null
      if (modelNode != null) {
        model = PercentileCalculationHelper.calculatePercentileByFieldName(modelGuidFieldName, SCORE_COUNT_FIELD_NAME,
            rawScoreFieldName, scoreFieldName, scoreFieldName, null,
            minPct, maxPct, modelNode, targetScoreDerivationInputs, targetScoreDerivationOutputs,
            originalScoreFieldMap, targetScoreDerivation, false)
      }
      if (evModelNode != null) {
          evModel = evModelNode.orderBy(List(modelGuidFieldName, rawScoreFieldName) map desc: _*)
      }

      var output: DataFrame = model
      if (model != null && evModel != null) {
          output = model.union(evModel)
      } else if (model == null) {
          output = evModel
      }
      
      writeTargetScoreDerivation(lattice, targetScoreDerivationOutputs)      
      lattice.output = output.select(inputTable.columns map col: _*) :: Nil
    }
    
    def writeTargetScoreDerivation(lattice: LatticeContext[RecalculatePercentileScoreJobConfig],
        targetScoreDerivationOutputs: MMap[String, String]) = {
      if (!targetScoreDerivationOutputs.isEmpty) {
        lattice.outputStr = serializeJson(targetScoreDerivationOutputs)
      }
    }


    def mergeCount(node: DataFrame, modelGuidFieldName: String): DataFrame = {
        val score: DataFrame = node.select(modelGuidFieldName)
        val totalCount: DataFrame = score
          .groupBy(modelGuidFieldName)
          .agg(count("*").as(SCORE_COUNT_FIELD_NAME))
        node.join(totalCount, Seq(modelGuidFieldName), "inner")
    
    }
}
