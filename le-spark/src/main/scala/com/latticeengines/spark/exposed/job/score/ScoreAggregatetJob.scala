package com.latticeengines.spark.exposed.job.score

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.serviceflows.core.spark.ScoreAggregateJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
 import org.apache.spark.sql._
import scala.collection.JavaConverters._

class ScoreAggregateJob extends AbstractSparkJob[ScoreAggregateJobConfig] {

    private val expectedRevenueField = InterfaceName.ExpectedRevenue.name
    private val probabilityField = InterfaceName.Probability.name
    private val avgField = InterfaceName.AverageScore.name
    
    override def runJob(spark: SparkSession, lattice: LatticeContext[ScoreAggregateJobConfig]): Unit = {

      val scoreResult: DataFrame = lattice.input.head
      val config: ScoreAggregateJobConfig = lattice.config
      var result = 
        if (config.scoreFieldMap != null) aggregateMultiModel(scoreResult, config) else aggregateSingleModel(scoreResult, config)
    
      lattice.output = result::Nil
    }
  
    def aggregateMultiModel(scoreResult: DataFrame, config: ScoreAggregateJobConfig) : DataFrame = {
      val modelGuidField = config.modelGuidField
      val scoreFieldMap = config.scoreFieldMap.asScala.toMap
      var scoredColumns = scoreFieldMap.values.toSet
      val selectedColumns = scoredColumns + modelGuidField
      
      var exprs = scoredColumns.map((_ -> "avg")).toMap
      val aggregateResult = scoreResult //
        .select(selectedColumns.toSeq.map(name=>col(name)): _*) //
        .na.fill(0) //
        .groupBy(modelGuidField) //
        .agg(exprs)
      val createAvgFunc: Row => Double = r => {
        val modelId = r.getAs[String](modelGuidField)
        val scoreField = scoreFieldMap(modelId)
        r.getAs[Double](s"avg($scoreField)")
      }
      
      val createAvgUdf = udf(createAvgFunc)
      aggregateResult.withColumn(avgField, createAvgUdf(struct(aggregateResult.columns map col: _*))).select(modelGuidField, avgField) 

    }
  
    def aggregateSingleModel(scoreResult: DataFrame, config: ScoreAggregateJobConfig) : DataFrame = {
      val scoreFieldName = 
        if (config.expectedValue != null && config.expectedValue) expectedRevenueField else probabilityField
      scoreResult //
        .select(scoreFieldName) //
        .agg(avg(scoreFieldName).as(avgField))
    }

}
