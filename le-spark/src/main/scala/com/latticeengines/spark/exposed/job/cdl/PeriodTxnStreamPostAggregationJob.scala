package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.spark.cdl.PeriodTxnStreamPostAggregationConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.{DeriveAttrsUtils, MergeUtils, TransactionSparkUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class PeriodTxnStreamPostAggregationJob extends AbstractSparkJob[PeriodTxnStreamPostAggregationConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[PeriodTxnStreamPostAggregationConfig]): Unit = {
    val analyticStream: DataFrame = lattice.input.head

    val periods: Seq[Int] = analyticStream.select(InterfaceName.PeriodId.name).distinct.rdd.map(r => r(0).asInstanceOf[Int]).collect
    val allPeriodsBetween = periods.min.to(periods.max)
    val missingPeriods: Seq[Int] = allPeriodsBetween.diff(periods)

    setPartitionTargets(0, Seq(InterfaceName.PeriodId.name), lattice)
    lattice.output = fillPeriodGaps(spark, analyticStream, missingPeriods) :: Nil
    lattice.outputStr = serializeJson(missingPeriods)
  }


  def fillPeriodGaps(spark: SparkSession, rawPeriodStream: DataFrame, missingPeriods: Seq[Int]): DataFrame = {
    if (missingPeriods.isEmpty) {
      return rawPeriodStream
    }
    val valueMap: Map[String, Any] = rawPeriodStream.first().getValuesMap(rawPeriodStream.columns)
    val missingRows: DataFrame = getMissingRows(spark, missingPeriods, valueMap)
    MergeUtils.concat2(TransactionSparkUtils.castMetricsColType(rawPeriodStream), missingRows).repartition(200, col(InterfaceName.PeriodId.name))
  }

  def getMissingRows(spark: SparkSession, periods: Seq[Int], valueMap: Map[String, Any]): DataFrame = {
    var rows: List[List[Any]] = List()
    periods.foreach(period => {
      val row: List[Any] = List( //
        valueMap(InterfaceName.AccountId.name), //
        valueMap(InterfaceName.ProductId.name), //
        valueMap(InterfaceName.TransactionType.name), //
        valueMap(InterfaceName.ProductType.name), // analytic
        period, //
        0L, 0.0, 0.0, 0.0, "0", 0L // rowCount, amount, quantity, cost, lastActivityDate, version
      )
      rows :+= row
    })

    val schema = StructType(List(
      StructField(InterfaceName.AccountId.name, StringType, nullable = false),
      StructField(InterfaceName.ProductId.name, StringType, nullable = false),
      StructField(InterfaceName.TransactionType.name, StringType, nullable = false),
      StructField(InterfaceName.ProductType.name, StringType, nullable = false),
      StructField(InterfaceName.PeriodId.name, IntegerType, nullable = false),
      StructField(InterfaceName.__Row_Count__.name, LongType, nullable = false),
      StructField(InterfaceName.Amount.name, DoubleType, nullable = true),
      StructField(InterfaceName.Quantity.name, DoubleType, nullable = true),
      StructField(InterfaceName.Cost.name, DoubleType, nullable = true),
      StructField(InterfaceName.LastActivityDate.name, StringType, nullable = true),
      StructField(DeriveAttrsUtils.VERSION_COL, LongType, nullable = true)
    ))

    val data: RDD[Row] = spark.sparkContext.parallelize(rows.map(row => Row(row: _*)))
    spark.createDataFrame(data, schema)
  }
}
