package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.metadata.transaction.ProductType
import com.latticeengines.domain.exposed.spark.cdl.DailyTxnStreamPostAggregationConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.{DeriveAttrsUtils, MergeUtils, TransactionSparkUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, first}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class DailyTxnStreamPostAggregationJob extends AbstractSparkJob[DailyTxnStreamPostAggregationConfig] {
  val productId: String = InterfaceName.ProductId.name
  val productBundle: String = InterfaceName.ProductBundle.name
  val productType: String = InterfaceName.ProductType.name
  val streamDateId: String = InterfaceName.StreamDateId.name

  val analytic: String = ProductType.Analytic.name
  val bundle: String = ProductType.Bundle.name

  override def runJob(spark: SparkSession, lattice: LatticeContext[DailyTxnStreamPostAggregationConfig]): Unit = {

    assert(lattice.input.size == 2)
    val rawDailyStream: DataFrame = lattice.input.head
    val productStore: DataFrame = lattice.input(1).select(productBundle, productId, productType)

    val allBundles: Seq[String] = productStore.filter(col(productType) === bundle && col(productBundle).isNotNull)
      .select(productBundle).distinct.rdd.map(r => r(0).asInstanceOf[String]).collect
    val existingBundles: Seq[String] = rawDailyStream
      .join(productStore.filter(col(productType) === analytic), Seq(productId), "left")
      .filter(col(productBundle).isNotNull) // analytic or spending products only have productBundle, no bundleId
      .select(productBundle).distinct.rdd.map(r => r(0).asInstanceOf[String]).collect
    val missingBundles: Seq[String] = allBundles diff existingBundles

    setPartitionTargets(0, Seq(streamDateId), lattice)
    lattice.output = fillMissingBundles(spark, rawDailyStream, missingBundles, productStore) :: Nil
    lattice.outputStr = serializeJson(missingBundles)
  }

  def fillMissingBundles(spark: SparkSession, rawDailyStream: DataFrame, missingBundles: Seq[String], productStore: DataFrame): DataFrame = {
    if (missingBundles.isEmpty) {
      // raw daily stream already repartitioned in agg step
      return rawDailyStream
    }

    val bundleProductIdReferenceMap = productStore
      .filter(col(productType) === analytic && col(productBundle).isInCollection(missingBundles) && col(productId).isNotNull)
      .groupBy(col(productBundle)).agg(first(productId).as(productId)).select(productBundle, productId).distinct
      .rdd.map(row => row.getString(0) -> row.getString(1)).collectAsMap()

    val valueMap = rawDailyStream.orderBy(streamDateId).first.getValuesMap(rawDailyStream.columns) // earliest date

    val schema = StructType(List(
      StructField(InterfaceName.AccountId.name, StringType, nullable = false),
      StructField(InterfaceName.ProductId.name, StringType, nullable = false),
      StructField(InterfaceName.TransactionType.name, StringType, nullable = false),
      StructField(InterfaceName.ProductType.name, StringType, nullable = false),
      StructField(InterfaceName.__StreamDate.name, StringType, nullable = false),
      StructField(InterfaceName.StreamDateId.name, IntegerType, nullable = false),
      StructField(InterfaceName.__Row_Count__.name, LongType, nullable = false),
      StructField(InterfaceName.Amount.name, DoubleType, nullable = true),
      StructField(InterfaceName.Quantity.name, DoubleType, nullable = true),
      StructField(InterfaceName.Cost.name, DoubleType, nullable = true),
      StructField(InterfaceName.LastActivityDate.name, StringType, nullable = true),
      StructField(DeriveAttrsUtils.VERSION_COL, LongType, nullable = true)
    ))

    var rows: List[List[Any]] = List()
    missingBundles.foreach(bundleName => {
      rows :+= List( //
        valueMap(InterfaceName.AccountId.name), //
        bundleProductIdReferenceMap(bundleName), //
        valueMap(InterfaceName.TransactionType.name), //
        valueMap(InterfaceName.ProductType.name), //
        valueMap(InterfaceName.__StreamDate.name), //
        valueMap(InterfaceName.StreamDateId.name), //
        0L, 0.0, 0.0, 0.0, "0", 0L // rowCount, amount, quantity, cost, lastActivityDate, version
      )
    })
    val data: RDD[Row] = spark.sparkContext.parallelize(rows.map(Row(_: _*)))
    MergeUtils.concat2(TransactionSparkUtils.castMetricsColType(rawDailyStream), spark.createDataFrame(data, schema)).repartition(200, col(streamDateId))
  }
}
