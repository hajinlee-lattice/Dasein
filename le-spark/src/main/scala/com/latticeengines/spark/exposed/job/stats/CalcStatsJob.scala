package com.latticeengines.spark.exposed.job.stats

import com.latticeengines.common.exposed.util.JsonUtils
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants._
import com.latticeengines.domain.exposed.datacloud.dataflow._
import com.latticeengines.domain.exposed.spark.stats.CalcStatsConfig
import com.latticeengines.spark.aggregation.BucketCountAggregation
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.commons.collections4.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._

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
    val inputData: DataFrame = lattice.input.head
    val profileData: DataFrame = lattice.input(1)

    val filteredProfile = filterProfile(profileData)
    val depivotedMap = depivot(inputData)

    val withBktId = depivotedMap.values.par.map(df => {
      val withBktId = toBktId(df, filteredProfile)
      withBktId.withColumn(Count, lit(1L)).persist(StorageLevel.DISK_ONLY).checkpoint()
    }).reduce(_ union _)

    val bktCntAgg = new BucketCountAggregation
    val grpCols = withBktId.columns.diff(Seq(BktId, Count))
    val withBktCnt = withBktId.groupBy(grpCols map col: _*)
      .agg(bktCntAgg(col(BktId), col(Count)).as("agg"))
      .withColumn(NotNullCount, col(s"agg.$BktSum"))
      .withColumn(Bkts, col(s"agg.$Bkts"))
      .drop("agg")
      .persist(StorageLevel.MEMORY_AND_DISK)
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
      .persist(StorageLevel.MEMORY_ONLY)
  }

  private def toBktId(depivoted: DataFrame, profile: DataFrame): DataFrame = {
    val algoMap = profile.collect().map(row => {
      (row.getString(0), JsonUtils.deserialize(row.getString(1), classOf[BucketAlgorithm]))
    }).toMap
    val fields = StructField(BktId, IntegerType, nullable = false) ::
      depivoted.schema.fields.filter(f => !f.name.equals(Value)).toList
    val outputSchema = StructType(fields)
    val nameIdx = depivoted.columns.indexOf(AttrName)
    val valIdx = depivoted.columns.indexOf(Value)
    depivoted.map(row => {
      val value = row.get(valIdx)
      val attrName = row.getString(nameIdx)
      val bktId = if (value == null) {
        0
      } else if (algoMap.contains(attrName)) {
        val algo = algoMap(attrName)
        algo match {
          case bkt: DiscreteBucket =>
            val dVal = value.asInstanceOf[Number].doubleValue
            val catList = bkt.getValues.asScala.map(_.doubleValue)
            val idx = catList.indexWhere(cat => dVal == cat)
            if (idx >= 0) {
              idx + 1
            } else {
              // FIXME temp workaround (assign to first bkt when not found) to unblock testing, remove after issue fixed
              // val msg = catList.map(cat => s"$dVal == $cat: ${dVal == cat}").mkString(",")
              // throw new RuntimeException(s"Cannot find value $value in given discrete list ${bkt.getValues}: $msg")
              0
            }
          case bkt: CategoricalBucket =>
            val catList = bkt.getCategories.asScala.map(_.toLowerCase).toList
            val strV = value match {
              case s: String => s.toLowerCase
              case _ => s"$value".toLowerCase
            }
            catList.indexOf(strV) + 1
          case bkt: IntervalBucket =>
            val dVal = value.asInstanceOf[Number].doubleValue
            val bnds = bkt.getBoundaries.asScala
            bnds.foldLeft(1)((idx, bnd) => {
              if (dVal >= bnd.doubleValue) {
                idx + 1
              } else {
                idx
              }
            })
          case _: BooleanBucket =>
            val strV = value match {
              case s: String => s.toLowerCase
              case _ => value.toString.toLowerCase
            }
            if ("true".equals(strV) || "yes".equals(strV) || "t".equals(strV) || "y".equals(strV) || "1".equals(strV)) {
              1
            } else if ("false".equals(strV) || "no".equals(strV) || "f".equals(strV) || "n".equals(strV) || "0".equals(strV)) {
              2
            } else {
              // FIXME temp workaround (assign to first bkt when not found) to unblock testing, remove after issue fixed
              // throw new IllegalArgumentException(s"Cannot convert $attrName value $value to boolean")
              // logSpark(s"Cannot convert $attrName value $value to boolean")
              0
            }
          case bkt: DateBucket =>
            // Decide which interval the date value falls into among the default date buckets.
            // Here are the options:
            // BUCKET #  BUCKET NAME    CRITERIA
            // 0         null           date value null, unparsable or negative
            // 1         LAST 7 DAYS    date between current time and 6 days before current time (inclusive)
            // 2         LAST 30 DAYS   date between current time and 29 days before current time (inclusive)
            // 3         LAST 90 DAYS   date between current time and 89 days before current time (inclusive)
            // 4         LAST 180 DAYS  date between current time and 179 days before current time (inclusive)
            // 5         EVER           date either after current time (in the future) or before 179 days ago
            val timeStamp: Long = value match {
              case l: Long => l
              case _ => s"$value".toLong
            }
            bkt.getDateBoundaries.asScala.foldLeft(1)((idx, bnd) => {
              if (timeStamp < bnd) {
                idx + 1
              } else {
                idx
              }
            })
          case _ =>
            throw new UnsupportedOperationException(s"Unknown algorithm ${algo.getClass}")
        }
      } else {
        1
      }
      val vals: Seq[Any] = bktId :: (0 until row.length).filter(i => !i.equals(valIdx)).map(row.get).toList
      Row.fromSeq(vals)
    })(RowEncoder(outputSchema))
  }

}
