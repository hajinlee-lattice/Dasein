package com.latticeengines.spark.exposed.job.stats

import com.latticeengines.common.exposed.util.JsonUtils
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants._
import com.latticeengines.domain.exposed.datacloud.dataflow._
import com.latticeengines.domain.exposed.spark.common.ChangeListConstants._
import com.latticeengines.domain.exposed.spark.stats.CalcStatsDeltaConfig
import com.latticeengines.spark.aggregation.BucketCountAggregation
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.BucketEncodeUtils
import org.apache.commons.collections4.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._

class CalcStatsDeltaJob extends AbstractSparkJob[CalcStatsDeltaConfig] {

  private val AttrName = STATS_ATTR_NAME
  private val SrcAttrName = PROFILE_ATTR_SRCATTR
  private val BktAlgo = PROFILE_ATTR_BKTALGO
  private val Bkts = STATS_ATTR_BKTS
  private val NotNullCount = STATS_ATTR_COUNT
  private val FromBktId = "FromBktId"
  private val ToBktId = "ToBktId"
  private val BktId = "BktId"
  private val BktSum = "BktSum"
  private val Count = "Count"

  override def runJob(spark: SparkSession, lattice: LatticeContext[CalcStatsDeltaConfig]): Unit = {
    val includeAttrs = if (lattice.config.getIncludeAttrs == null) None else Some(lattice.config.getIncludeAttrs.toSeq)

    val changelist = includeAttrs match {
      case None => lattice.input.head
      case Some(attrs) =>
        val bAttrs = spark.sparkContext.broadcast(attrs.toSet)
        val cidIdx = lattice.input.head.columns.indexOf(ColumnId)
        lattice.input.head.filter(row => bAttrs.value.contains(row.getString(cidIdx)))
    }

    val noRemoveCols = ignoreRemovedColumns(changelist)
    val cells = keepCells(noRemoveCols)

    val profile = filterProfile(lattice.input(1))
    val withBktIds = toBktIds(spark, cells, profile).persist(StorageLevel.DISK_ONLY)

    val fromBkts = cntBkt(withBktIds, FromBktId).withColumn(Count, col(Count) * -1)
    val toBkts = cntBkt(withBktIds, ToBktId)
    val bktCntAgg = new BucketCountAggregation
    val bktCnts = (fromBkts union toBkts).groupBy(AttrName) //
      .agg(bktCntAgg(col(BktId), col(Count)).as("agg")) //
      .withColumn(NotNullCount, col(s"agg.$BktSum")) //
      .withColumn(Bkts, col(s"agg.$Bkts")) //
      .drop("agg")

    val result = bktCnts.join(profile.select(AttrName, BktAlgo), Seq(AttrName), joinType = "left")
      .withColumn(NotNullCount, when(col(NotNullCount).isNull, lit(0L)).otherwise(col(NotNullCount)))
      .orderBy(AttrName)

    lattice.output = List(result)
  }

  private def ignoreRemovedColumns(changelist: DataFrame): DataFrame = {
    val removeCols = changelist.filter(col(RowId).isNull && col(Deleted) === true).select(ColumnId).distinct()
    changelist.join(broadcast(removeCols).alias("rhs"), Seq(ColumnId), joinType = "left")
      .filter(col(s"rhs.$ColumnId").isNull)
  }

  // remove row-# and #-col entries, then dedupe on row-col
  private def keepCells(changelist: DataFrame): DataFrame = {
    changelist.filter(col(RowId).isNotNull && col(ColumnId).isNotNull)
  }

  private def cntBkt(withBktIds: DataFrame, bktIdCol: String): DataFrame = {
    withBktIds.select(col(AttrName), col(bktIdCol))
      .withColumnRenamed(bktIdCol, BktId).filter(col(BktId) =!= 0)
      .groupBy(AttrName, BktId).count().withColumnRenamed("count", Count)
  }

  private def toBktIds(spark: SparkSession, changelist: DataFrame, profile: DataFrame): DataFrame = {
    val algoMap = profile.collect().map(row => {
      (row.getString(0), JsonUtils.deserialize(row.getString(1), classOf[BucketAlgorithm]))
    }).toMap
    val outputSchema = StructType(List(
      StructField(AttrName, StringType, nullable = false),
      StructField(FromBktId, IntegerType, nullable = false),
      StructField(ToBktId, IntegerType, nullable = false)
    ))
    val bktIdFunc = spark.sparkContext.broadcast(BucketEncodeUtils.bucketFunc(algoMap))
    val dTypeIdx = changelist.columns.indexOf(DataType)
    val nameIdx = changelist.columns.indexOf(ColumnId)
    val fromIdx = Map(
      "String" -> changelist.columns.indexOf(s"${From}String"),
      "Boolean" -> changelist.columns.indexOf(s"${From}Boolean"),
      "Integer" -> changelist.columns.indexOf(s"${From}Integer"),
      "Long" -> changelist.columns.indexOf(s"${From}Long"),
      "Float" -> changelist.columns.indexOf(s"${From}Float"),
      "Double" -> changelist.columns.indexOf(s"${From}Double")
    )
    val toIdx = Map(
      "String" -> changelist.columns.indexOf(s"${To}String"),
      "Boolean" -> changelist.columns.indexOf(s"${To}Boolean"),
      "Integer" -> changelist.columns.indexOf(s"${To}Integer"),
      "Long" -> changelist.columns.indexOf(s"${To}Long"),
      "Float" -> changelist.columns.indexOf(s"${To}Float"),
      "Double" -> changelist.columns.indexOf(s"${To}Double")
    )
    changelist.flatMap(row => {
      val attrName = row.getString(nameIdx)
      val dType = row.getString(dTypeIdx)
      val fromValueIdx = fromIdx(dType)
      val toValueIdx = toIdx(dType)
      val fromValue = row.get(fromValueIdx)
      val toValue = row.get(toValueIdx)
      val fromBktId = bktIdFunc.value.apply(attrName, fromValue)
      val toBktId = bktIdFunc.value.apply(attrName, toValue)
      if (fromBktId == toBktId) {
        None
      } else {
        Some(Row.fromSeq(Seq(attrName, fromBktId, toBktId)))
      }
    })(RowEncoder(outputSchema))
  }

  private def filterProfile(profile: DataFrame): DataFrame = {
    val shouldBkt = udf[Boolean, String](algoStr => {
      if (StringUtils.isBlank(algoStr)) {
        false
      } else {
        val algo = JsonUtils.deserialize(algoStr, classOf[BucketAlgorithm])
        algo match {
          case bkt: DiscreteBucket =>
            CollectionUtils.isNotEmpty(bkt.getValues)
          case bkt: CategoricalBucket =>
            CollectionUtils.isNotEmpty(bkt.getCategories)
          case bkt: IntervalBucket =>
            CollectionUtils.isNotEmpty(bkt.getBoundaries)
          case _ =>
            true
        }
      }
    })
    profile.filter(shouldBkt(col(BktAlgo)))
      .withColumn(AttrName, coalesce(col(SrcAttrName), col(AttrName)))
      .select(AttrName, BktAlgo)
      .orderBy(AttrName)
      .persist(StorageLevel.MEMORY_AND_DISK)
  }

}
