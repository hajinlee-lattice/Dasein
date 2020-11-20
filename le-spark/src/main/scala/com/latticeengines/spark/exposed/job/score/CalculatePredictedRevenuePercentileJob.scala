package com.latticeengines.spark.exposed.job.score

import com.latticeengines.domain.exposed.cdl.scoring.LookupPercentileForRevenueFunction2
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.CalculatePredictedRevenuePercentileJobConfig
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.CalculateExpectedRevenuePercentileJobConfig.ScoreDerivationType
import com.latticeengines.domain.exposed.scoring.ScoreResultField
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation
import com.latticeengines.spark.exposed.job.{ AbstractSparkJob, LatticeContext }
import org.apache.spark.sql.{ DataFrame, SparkSession }
import scala.collection.JavaConverters._
import org.apache.spark.sql.functions.{ col, lit, udf, desc }
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

class CalculatePredictedRevenuePercentileJob extends AbstractSparkJob[CalculatePredictedRevenuePercentileJobConfig] {

  private var context: ParsedContext = _

  override def runJob(spark: SparkSession, lattice: LatticeContext[CalculatePredictedRevenuePercentileJobConfig]): Unit = {

    val config: CalculatePredictedRevenuePercentileJobConfig = lattice.config
    var inputTable: DataFrame = lattice.input.head
  
    val percentileFieldName = config.percentileFieldName
    val modelGuidFieldName = config.modelGuidField 
    val originalScoreFieldMap: Map[String, String] = config.originalScoreFieldMap.asScala.toMap
    val minPct: Integer = config.percentileLowerBound
    val maxPct: Integer = config.percentileUpperBound
    val predictedRevenueFieldName = ScoreResultField.PredictedRevenue.displayName
    val scoreDerivationMaps: Map[String, java.util.Map[ScoreDerivationType, ScoreDerivation]] = if (config.scoreDerivationMaps == null) Map() else config.scoreDerivationMaps.asScala.toMap
    
    var addPercentileColumn = inputTable.withColumn(percentileFieldName, lit(null).cast(IntegerType))
    val retainedFields = addPercentileColumn.columns

    if (!originalScoreFieldMap.isEmpty) {
          var calculatePercentile = lookupPercentileFromScoreDerivation(scoreDerivationMaps, originalScoreFieldMap,
                  modelGuidFieldName, percentileFieldName, minPct, maxPct, addPercentileColumn);
          calculatePercentile = calculatePercentile.select(retainedFields map col: _*)
          lattice.output = calculatePercentile :: Nil
    } else {
      lattice.output = addPercentileColumn :: Nil
    }
  }
  
  def lookupPercentileFromScoreDerivation(
            scoreDerivationMap: Map[String, java.util.Map[ScoreDerivationType, ScoreDerivation]],
            originalScoreFieldMap: Map[String, String],
            modelGuidFieldName: String, percentileFieldName: String, minPct: Integer, maxPct: Integer, input: DataFrame): DataFrame = {

        val inputCache = input.persist(StorageLevel.DISK_ONLY)
        var merged: DataFrame = null
        for ((modelGuid, scoreField) <- originalScoreFieldMap) {
            var node = inputCache.filter(col(modelGuidFieldName) === modelGuid)
            if (scoreDerivationMap.contains(modelGuid)) {
              val calFunc = new LookupPercentileForRevenueFunction2(scoreDerivationMap(modelGuid).get(ScoreDerivationType.REVENUE))
              val calFuncUdf = udf((revenue : Double) => {
                calFunc.calculate(revenue)
              })
              node = node.withColumn(percentileFieldName, calFuncUdf(col(scoreField)))
              node = node.orderBy(List(percentileFieldName, scoreField) map desc: _*)
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

