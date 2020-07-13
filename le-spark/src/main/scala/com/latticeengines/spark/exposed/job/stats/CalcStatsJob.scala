package com.latticeengines.spark.exposed.job.stats

import com.latticeengines.common.exposed.util.JsonUtils
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants._
import com.latticeengines.domain.exposed.datacloud.dataflow._
import com.latticeengines.domain.exposed.spark.stats.CalcStatsConfig
import com.latticeengines.spark.aggregation.BucketCountAggregation
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.BucketEncodeUtils
import org.apache.commons.collections4.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._

class CalcStatsJob extends AbstractSparkJob[CalcStatsConfig] {

  private val AttrName = STATS_ATTR_NAME
  private val SrcAttrName = PROFILE_ATTR_SRCATTR
  private val BktAlgo = PROFILE_ATTR_BKTALGO
  private val Bkts = STATS_ATTR_BKTS
  private val NotNullCount = STATS_ATTR_COUNT
  private val Value = "Value"
  private val BktId = "BktId"
  private val BktSum = "BktSum"
  private val Count = "Count"

  override def runJob(spark: SparkSession, lattice: LatticeContext[CalcStatsConfig]): Unit = {
    val includeAttrs = if (lattice.config.getIncludeAttrs == null) None else Some(lattice.config.getIncludeAttrs.toSeq)

    val inputData: DataFrame = includeAttrs match {
      case None =>  lattice.input.head
      case Some(attrs) =>
        val retainAttrs = lattice.input.head.columns.intersect(attrs)
        lattice.input.head.select(retainAttrs map col:_*)
    }
    val profileData: DataFrame = lattice.input(1)

    val filteredProfile = filterProfile(profileData)
    val depivotedMap = depivot(inputData)

    val withBktCnt = depivotedMap.values.par.map(df => {
      val withBktId = toBktId(spark, df, filteredProfile)
      val withBktCnt = withBktId.groupBy(AttrName, BktId).count().withColumnRenamed("count", Count)
      val bktCntAgg = new BucketCountAggregation
      withBktCnt.groupBy(AttrName) //
        .agg(bktCntAgg(col(BktId), col(Count)).as("agg")) //
        .withColumn(NotNullCount, col(s"agg.$BktSum")) //
        .withColumn(Bkts, col(s"agg.$Bkts")) //
        .drop("agg") //
        .persist(StorageLevel.MEMORY_AND_DISK)
    }).reduce(_ union _)

    val allColsRdd: RDD[Row] = spark.sparkContext.parallelize(inputData.columns.map(c => Row.fromSeq(Seq(c))))
    val allColsDf = spark.createDataFrame(allColsRdd, StructType(List(StructField(AttrName, StringType))))
    val rhs = filteredProfile.select(AttrName, BktAlgo)
    val result = allColsDf.join(withBktCnt, Seq(AttrName), joinType = "left").join(rhs, Seq(AttrName), joinType = "left")
      .withColumn(NotNullCount, when(col(NotNullCount).isNull, lit(0L)).otherwise(col(NotNullCount)))
      .orderBy(AttrName)

    lattice.output = result :: Nil
  }

  private def depivot(df: DataFrame): Map[DataType, DataFrame] = {
    val schemaMap = df.schema.groupBy(field => field.dataType)
    schemaMap.map(t => {
      val (dType, fields) = t
      val attrs: List[String] = fields.map(_.name).toList
      val depivoted = depivotCols(df, attrs, dType)
      (dType, depivoted)
    })
  }

  private def depivotCols(df: DataFrame, attrs: Seq[String], dataType: DataType): DataFrame = {
    val outputFields =
      List(StructField(AttrName, StringType, nullable = false), StructField(Value, dataType, nullable = true))
    df.select(attrs map col: _*).flatMap(row => {
      attrs.flatMap(attr => {
        val attrVal = row.getAs[Any](attr)
        attrVal match {
          case null => List()
          case None => List()
          case Some(obj) => List(Row.fromSeq(Seq(attr, obj)))
          case _ => List(Row.fromSeq(Seq(attr, attrVal)))
        }
      })
    })(RowEncoder(StructType(outputFields)))
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

  private def toBktId(spark: SparkSession, depivoted: DataFrame, profile: DataFrame): DataFrame = {
    val algoMap = profile.collect().map(row => {
      (row.getString(0), JsonUtils.deserialize(row.getString(1), classOf[BucketAlgorithm]))
    }).toMap
    val fields = StructField(BktId, IntegerType, nullable = false) ::
      depivoted.schema.fields.filter(f => !f.name.equals(Value)).toList
    val outputSchema = StructType(fields)
    val nameIdx = depivoted.columns.indexOf(AttrName)
    val valIdx = depivoted.columns.indexOf(Value)
    val bktIdFunc = spark.sparkContext.broadcast(BucketEncodeUtils.bucketFunc(algoMap))
    depivoted.flatMap(row => {
      val value = row.get(valIdx)
      val attrName = row.getString(nameIdx)
      val bktId = bktIdFunc.value.apply(attrName, value)
      if (bktId != 0) {
        val vals: Seq[Any] = bktId :: (0 until row.length).filter(i => !i.equals(valIdx)).map(row.get).toList
        Some(Row.fromSeq(vals))
      } else {
        None
      }
    })(RowEncoder(outputSchema))
  }

}
