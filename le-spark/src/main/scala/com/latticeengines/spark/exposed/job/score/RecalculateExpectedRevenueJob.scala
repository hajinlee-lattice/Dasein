package com.latticeengines.spark.exposed.job.score

import com.latticeengines.domain.exposed.serviceflows.scoring.spark.RecalculateExpectedRevenueJobConfig
import com.latticeengines.spark.exposed.job.{ AbstractSparkJob, LatticeContext }
import com.latticeengines.domain.exposed.cdl.scoring.CalculateExpectedRevenueFunction2
import com.latticeengines.domain.exposed.scoring.ScoreResultField
import org.apache.spark.sql.{ DataFrame, SparkSession }
import scala.collection.JavaConverters._
import org.apache.spark.sql.functions.{ col, udf }
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

class RecalculateExpectedRevenueJob extends AbstractSparkJob[RecalculateExpectedRevenueJobConfig] {

    override def runJob(spark: SparkSession, lattice: LatticeContext[RecalculateExpectedRevenueJobConfig]): Unit = {
      val config: RecalculateExpectedRevenueJobConfig = lattice.config
      var inputTable: DataFrame = lattice.input.head
     
      val originalFields = inputTable.columns
      val modelGuidFieldName = config.modelGuidField
      val expectedRevenueFieldName = config.expectedRevenueFieldName
      val percentileFieldName = config.percentileFieldName
      val predictedRevenuePercentileFieldName = config.predictedRevenuePercentileFieldName
      val originalScoreFieldMap: Map[String, String] = config.originalScoreFieldMap.asScala.toMap
      val fitFunctionParametersMap: Map[String, String] = if (config.fitFunctionParametersMap == null) Map() else config.fitFunctionParametersMap.asScala.toMap 
      if (!fitFunctionParametersMap.isEmpty) {
            lattice.output = calculateExpectedRevenueByFieldMap(originalScoreFieldMap, fitFunctionParametersMap,
                    modelGuidFieldName, percentileFieldName, predictedRevenuePercentileFieldName,
                    expectedRevenueFieldName, inputTable).select(originalFields map col: _*) :: Nil
        } else {
            lattice.output = inputTable :: Nil 
        }
    }
    
    def calculateExpectedRevenueByFieldMap(originalScoreFieldMap: Map[String, String],
            fitFunctionParametersMap: Map[String, String], modelGuidFieldName: String, percentileFieldName: String,
            predictedRevenuePercentileFieldName: String, expectedRevenueFieldName: String, input: DataFrame): DataFrame = {
      
        val inputCache = input.persist(StorageLevel.DISK_ONLY)
        var merged: DataFrame = null
        for ((modelGuid, scoreField) <- originalScoreFieldMap) {
            var node = inputCache.filter(col(modelGuidFieldName) === modelGuid)
            val evFitFunctionParameterStr = fitFunctionParametersMap.get(modelGuid)
            val isEV = (ScoreResultField.ExpectedRevenue.displayName == scoreField)
             println("----- BEGIN SCRIPT OUTPUT -----")
             println(s"modelGuid=${modelGuid}, isEV=${isEV}")
             println("----- END SCRIPT OUTPUT -----")
            val output: DataFrame = 
                if (isEV) calculatePercentileAndFittedExpectedRevenue(percentileFieldName,
                    predictedRevenuePercentileFieldName, expectedRevenueFieldName, evFitFunctionParameterStr.orNull, node)
                else node
            if (merged == null) {
                merged = output
            } else {
                merged = merged.union(output)
            }
        }
        return merged
    }

    def calculatePercentileAndFittedExpectedRevenue(percentileFieldName: String,
            predictedRevenuePercentileFieldName: String, expectedRevenueFieldName: String,
            evFitFunctionParameterStr: String, node: DataFrame): DataFrame = {
         
        val calFunc = new CalculateExpectedRevenueFunction2(evFitFunctionParameterStr)
        val calFuncUdf = udf((percentile: Integer, predictedRevenuePercentile: Integer) => {
                  calFunc.calculate(percentile, predictedRevenuePercentile)
            })
        val result = node.withColumn("CalResult", calFuncUdf(col(percentileFieldName), col(predictedRevenuePercentileFieldName)))
                .withColumn(expectedRevenueFieldName, col("CalResult")(0))
                .withColumn(ScoreResultField.Probability.displayName, col("CalResult")(1))
                .drop("CalResult")
        result
    }
}
