package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.spark.cdl.DailyTxnStreamPostAggregationConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.{DeriveAttrsUtils, MergeUtils, TransactionUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, first}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class DailyTxnStreamPostAggregationJob extends AbstractSparkJob[DailyTxnStreamPostAggregationConfig] {
  val productId: String = InterfaceName.ProductId.name
  val productBundleId: String = InterfaceName.ProductBundleId.name
  val streamDateId: String = InterfaceName.StreamDateId.name

  override def runJob(spark: SparkSession, lattice: LatticeContext[DailyTxnStreamPostAggregationConfig]): Unit = {

    assert(lattice.input.size == 2)
    val rawDailyStream: DataFrame = lattice.input.head
    val productBundleReference: DataFrame = lattice.input(1).select(productId, productBundleId).filter(col(productBundleId).isNotNull).distinct

    val existingBundles: Seq[String] = rawDailyStream.join(productBundleReference, Seq(productId), "left").filter(col(productBundleId).isNotNull)
      .select(productBundleId).distinct.rdd.map(r => r(0).asInstanceOf[String]).collect
    val allBundles: Seq[String] = productBundleReference.select(productBundleId).distinct.rdd.map(r => r(0).asInstanceOf[String]).collect
    val missingBundles: Seq[String] = allBundles diff existingBundles

    setPartitionTargets(0, Seq(streamDateId), lattice)
    val result = fillMissingBundles(spark, rawDailyStream, missingBundles, productBundleReference)
    result.show(false)
    lattice.output = result :: Nil
  }

  def fillMissingBundles(spark: SparkSession, rawDailyStream: DataFrame, missingBundles: Seq[String], productBundleReference: DataFrame): DataFrame = {
    if (missingBundles.isEmpty) {
      // raw daily stream already repartitioned in agg step
      return rawDailyStream
    }

    val productBundleReferenceMap = productBundleReference.filter(col(productBundleId).isInCollection(missingBundles))
      .groupBy(col(productBundleId)).agg(first(productId).as(productId)).select(productBundleId, productId).distinct
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
    missingBundles.foreach(bundleId => {
      rows :+= List( //
        valueMap(InterfaceName.AccountId.name), //
        productBundleReferenceMap(bundleId), //
        valueMap(InterfaceName.TransactionType.name), //
        valueMap(InterfaceName.ProductType.name), //
        valueMap(InterfaceName.__StreamDate.name), //
        valueMap(InterfaceName.StreamDateId.name), //
        0L, 0.0, 0.0, 0.0, "0", 0L // rowCount, amount, quantity, cost, lastActivityDate, version
      )
    })
    val data: RDD[Row] = spark.sparkContext.parallelize(rows.map(Row(_: _*)))
    MergeUtils.concat2(TransactionUtils.castMetricsColType(rawDailyStream), spark.createDataFrame(data, schema)).repartition(200, col(streamDateId))
  }
}
