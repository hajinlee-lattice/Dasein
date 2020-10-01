package com.latticeengines.spark.exposed.job.score

import org.apache.commons.collections4.MapUtils

import com.latticeengines.domain.exposed.serviceflows.scoring.spark.CalculateExpectedRevenuePercentileJobConfig
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.CalculateExpectedRevenuePercentileJobConfig.ScoreDerivationType
import com.latticeengines.domain.exposed.scoring.ScoreResultField
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation
import com.latticeengines.spark.exposed.job.{ AbstractSparkJob, LatticeContext }
import org.apache.spark.sql.{ DataFrame, SparkSession }
import scala.collection.JavaConverters._
import org.apache.spark.sql.functions.{ col, when, lit, count }
import org.apache.spark.sql.types._

class CalculateExpectedRevenuePercentileJob extends AbstractSparkJob[CalculateExpectedRevenuePercentileJobConfig] {

  private var context: ParsedContext = _

  override def runJob(spark: SparkSession, lattice: LatticeContext[CalculateExpectedRevenuePercentileJobConfig]): Unit = {

    val config: CalculateExpectedRevenuePercentileJobConfig = lattice.config
    var inputTable: DataFrame = lattice.input.head
    context = ParsedContext(config)

    val addPercentileColumn: DataFrame = inputTable.withColumn(context.percentileFieldName, lit(null).cast(IntegerType))
    var retainedFields = addPercentileColumn.columns

    if (!context.originalScoreFieldMap.isEmpty) {
      val mergedScoreCount = mergeCount(context, addPercentileColumn)

      // sort based on ExpectedRevenue column (and using number of
      // rows) calculate percentile and put value in new ExpectedRevenuePercentile field
      // ExpectedRevenue => ExpectedRevenuePercentile
      var calculatePercentile: DataFrame = PercentileCalculationHelper.calculate(context, mergedScoreCount, false, false)
      calculatePercentile = calculatePercentile.select(retainedFields map col: _*)

      println("----- BEGIN SCRIPT OUTPUT -----")
      println(s"percentileFieldName=${context.percentileFieldName}, standardScoreField=${context.standardScoreField}")
      println("----- END SCRIPT OUTPUT -----")

      if (context.standardScoreField != context.percentileFieldName) {
        // populate ExpectedRevenuePercentile => outputPercentileFieldNamef
        retainedFields = calculatePercentile.columns
        calculatePercentile = calculatePercentile.withColumn( //
          context.outputPercentileFieldName, //
          when(col(context.percentileFieldName).isNotNull, col(context.percentileFieldName))
            .otherwise(col(context.standardScoreField)))

        // Use "ev" field from evScoreDerivation.json to lookup for
        // temporary EV percentile score for average expected revenue
        // value.
        // lookup expectedRevenueField => outputPercentileFieldName  // EV Model Only

        calculatePercentile = PercentileLookupEvHelper.calculate(context, calculatePercentile)

        // leaving the code for taking backup of PredictedRev and
        // Probability so that we can easily enable it in future. To
        // enable the backup just move next two lines between
        // declaration of Node inputTable and Node addPercentileColumn
        // and update Node references accordingly

        val addBackupPredictedRevColumn = calculatePercentile
          .withColumn(context.backupPredictedRevFieldName, col(context.predictedRevenueField))
        val addBackupProbabilityColumn = addBackupPredictedRevColumn
          .withColumn(context.backupProbabilityFieldName, col(context.probabilityField))

        // initialize expectedRevenueFitter based on corresponding
        // fit function parameters
        //
        // for each row
        // calculate fitted expected revenue using this new
        // temporary EV percentile score and set it back to
        // ExpectedRevenue column
        //
        // copy values of ExpectedRevenuePercentile in original
        // percentile column ("Score") as downstream processing expects
        // final percentiles into original percentile column

        calculatePercentile = calculateFittedExpectedRevenue(retainedFields.toList, addBackupProbabilityColumn)
        calculatePercentile = calculateFinalPercentile(retainedFields, calculatePercentile)

        calculatePercentile
      }
      writeTargetScoreDerivation(context, lattice)
      lattice.output = calculatePercentile :: Nil
    } else {
      lattice.output = addPercentileColumn :: Nil
    }
  }

  def writeTargetScoreDerivation(context: ParsedContext, lattice: LatticeContext[CalculateExpectedRevenuePercentileJobConfig]) = {
    if (!context.targetScoreDerivationOutputs.isEmpty) {
      lattice.outputStr = serializeJson(context.targetScoreDerivationOutputs)
    }
  }

  def calculateFinalPercentile(retainedFields: Array[String], calculatePercentileInput: DataFrame): DataFrame = {
    var calculatePercentile = calculatePercentileInput.withColumn(

      context.percentileFieldName, lit(null).cast(IntegerType))
    val mergedScoreCount = mergeCount(context, calculatePercentile)
    calculatePercentile = PercentileCalculationHelper
      .calculate(context, mergedScoreCount, true, context.targetScoreDerivation)
      .select(retainedFields map col: _*)

    calculatePercentile = calculatePercentile //
      .withColumn(
        context.PREFIX_TEMP_COL + context.standardScoreField,
        when(col(context.percentileFieldName).isNotNull, col(context.percentileFieldName))
          .otherwise(col(context.standardScoreField)))
    calculatePercentile = calculatePercentile.drop(context.standardScoreField)
    calculatePercentile = calculatePercentile.withColumnRenamed(
      context.PREFIX_TEMP_COL + context.standardScoreField,
      context.standardScoreField)

    println("----- BEGIN SCRIPT OUTPUT -----")
    println(s"context.standardScoreField=${context.standardScoreField}")
    println("----- END SCRIPT OUTPUT -----")

    calculatePercentile
  }

  def calculateFittedExpectedRevenue(retainedFields: List[String], calculatePercentileInput: DataFrame): DataFrame = {
    var calculatePercentile = calculatePercentileInput //
      .withColumn(context.outputExpRevFieldName, col(context.expectedRevenueField))
    var tempRetainedFieldList = retainedFields
    tempRetainedFieldList = tempRetainedFieldList :+ context.outputExpRevFieldName
    CalculateFittedExpectedRevenueHelper.calculate(context, calculatePercentile, tempRetainedFieldList)
  }

  def mergeCount(context: ParsedContext, node: DataFrame): DataFrame = {
    val score: DataFrame = node.select(context.modelGuidFieldName)
    val totalCount: DataFrame = score
      .groupBy(context.modelGuidFieldName)
      .agg(count("*").as(context.scoreCountFieldName))
    node.join(totalCount, Seq(context.modelGuidFieldName), "inner")
  }

}

