package com.latticeengines.spark.exposed.job.stats

import com.latticeengines.domain.exposed.datacloud.DataCloudConstants
import com.latticeengines.domain.exposed.datacloud.dataflow.stats.ProfileParameters.Attribute
import com.latticeengines.domain.exposed.datacloud.dataflow.{CategoricalBucket, DiscreteBucket, IntervalBucket}
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook
import com.latticeengines.domain.exposed.spark.stats.ProfileJobConfig
import com.latticeengines.spark.aggregation.{NumberProfileAggregation, StringProfileAggregation}
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.{BitEncodeUtils, NumericProfiler}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, explode, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._

class ProfileJob extends AbstractSparkJob[ProfileJobConfig] {

  private val STR_CHUNK_SIZE: Int = 1000
  private val NUM_CHUNK_SIZE: Int = 100
  private val STR_APPROACH_THRESHOLD: Long = 1000000 // (1M) using attr-by-attr approach when more than this # of rows
  private val NUM_APPROACH_THRESHOLD: Long = 10000 // (10K) using attr-by-attr approach when more than this # of rows
  private val ATTR_ATTRNAME = DataCloudConstants.PROFILE_ATTR_ATTRNAME
  private val ATTR_BKTALGO = DataCloudConstants.PROFILE_ATTR_BKTALGO

  private val NUM_TYPES: Set[DataType] = Set(IntegerType, LongType, FloatType, DoubleType)

  override def runJob(spark: SparkSession, lattice: LatticeContext[ProfileJobConfig]): Unit = {
    val inputData: DataFrame = lattice.input.head
    val config: ProfileJobConfig = lattice.config
    val numAttrs = getNumAttrs(inputData, config)
    val randomSeed: Long = if (config.getRandSeed == null) System.currentTimeMillis else config.getRandSeed
    val enforceByAttr: Boolean = if (config.getEnforceProfileByAttr == null) false else config.getEnforceProfileByAttr
    val totalCnt = inputData.count
    val numProfile = if (enforceByAttr || totalCnt > NUM_APPROACH_THRESHOLD) {
      profileNumAttrs(spark, totalCnt, numAttrs,
        config.getMaxDiscrete, config.getBucketNum, config.getMinBucketSize, randomSeed)
    } else {
      profileNumAttrsByGroup(numAttrs, numAttrs.columns,
        config.getMaxDiscrete, config.getBucketNum, config.getMinBucketSize, randomSeed)
    }

    val catAttrs: List[String] =
      if (config.getCatAttrs == null) List() else config.getCatAttrs.asScala.map(_.getAttr).toList
    val strProfile = if (enforceByAttr || totalCnt > STR_APPROACH_THRESHOLD) {
      profileStrAttrs(spark, inputData, catAttrs, config.getMaxCat, config.getMaxCatLength)
    } else {
      profileStrAttrsByGroup(inputData, catAttrs, config.getMaxCat, config.getMaxCatLength)
    }

    val result = numProfile.union(strProfile)
            .withColumn(DataCloudConstants.PROFILE_ATTR_SRCATTR, lit(null).cast("string"))
            .withColumn(DataCloudConstants.PROFILE_ATTR_DECSTRAT, lit(null).cast("string"))
            .withColumn(DataCloudConstants.PROFILE_ATTR_ENCATTR, lit(null).cast("string"))
            .withColumn(DataCloudConstants.PROFILE_ATTR_LOWESTBIT, lit(null).cast("integer"))
            .withColumn(DataCloudConstants.PROFILE_ATTR_NUMBITS, lit(null).cast("integer"))
            .select(List(
              DataCloudConstants.PROFILE_ATTR_ATTRNAME,
              DataCloudConstants.PROFILE_ATTR_SRCATTR,
              DataCloudConstants.PROFILE_ATTR_DECSTRAT,
              DataCloudConstants.PROFILE_ATTR_ENCATTR,
              DataCloudConstants.PROFILE_ATTR_LOWESTBIT,
              DataCloudConstants.PROFILE_ATTR_NUMBITS,
              DataCloudConstants.PROFILE_ATTR_BKTALGO
            ).map(col) : _*)

     lattice.output = result :: Nil
  }

  private def getNumAttrs(input: DataFrame, config: ProfileJobConfig): DataFrame = {
    val numAttrs: List[Attribute] = //
      if (config.getNumericAttrs == null) List() else config.getNumericAttrs.asScala.toList

    val codeBookLookup: Map[String, String] = //
      if (config.getCodeBookLookup == null) Map() else config.getCodeBookLookup.asScala.toMap

    val codeBookMap: Map[String, BitCodeBook] = //
      if (config.getCodeBookMap == null) Map() else config.getCodeBookMap.asScala.toMap

    val numCols: Seq[String] = numAttrs.map(_.getAttr)
    val output = BitEncodeUtils.decode(input, numCols, codeBookLookup, codeBookMap)
    output.schema.foreach(field => {
      if (!NUM_TYPES.contains(field.dataType)) {
        throw new IllegalArgumentException(s"Attribute ${field.name} of type ${field.dataType} is not numerical.")
      }
    })
    output
  }

  private def profileStrAttrs(spark: SparkSession, input: DataFrame, catAttrs: List[String],
                              maxCats: Int, maxLength: Int): DataFrame = {
    val rdd: RDD[Row] = spark.sparkContext.parallelize(catAttrs.map(catAttr => {
      val catCnts = input.filter(col(catAttr).isNotNull && col(catAttr) =!= "")
        .groupBy(catAttr).count().limit(maxCats + 2).collect()
      val nCats = catCnts.length
      val bkgAlgo = if (nCats <= maxCats) {
        val profile = catCnts.map(row => (row.getString(0), row.getLong(1))).sortBy(t => t._1).toList
        val longestCat = if(profile.isEmpty) 0 else profile.map(_._1.length).max
        if (longestCat > maxLength) {
          null
        } else {
          val bkt: CategoricalBucket = new CategoricalBucket
          bkt.setCategories(profile.map(_._1).asJava)
          bkt.setCounts(profile.map(_._2).asInstanceOf[List[java.lang.Long]].asJava)
          serializeJson(bkt)
        }
      } else {
        null
      }
      Row.fromSeq(Seq(catAttr, bkgAlgo))
    }))
    val outputSchema = StructType(List(
      StructField(ATTR_ATTRNAME, StringType),
      StructField(ATTR_BKTALGO, StringType)
    ))
    spark.createDataFrame(rdd, outputSchema).persist(StorageLevel.MEMORY_AND_DISK)
  }

  private def profileStrAttrsByGroup(input: DataFrame, catAttrs: List[String],
                                     maxCats: Int, maxLength: Int): DataFrame = {
    if (catAttrs.length <= STR_CHUNK_SIZE) {
      input.select(catAttrs map col: _*)
      val aggregate = new StringProfileAggregation(catAttrs, maxCats, maxLength)
      input.agg(aggregate(catAttrs map col: _*).as("agg"))
        .withColumn("agg", explode(col("agg")))
        .withColumn(ATTR_ATTRNAME, col("agg.attr"))
        .withColumn(ATTR_BKTALGO, col("agg.algo"))
        .drop("agg")
    } else {
      val (head, tail) = catAttrs.splitAt(STR_CHUNK_SIZE)
      val headProfile: DataFrame = profileStrAttrsByGroup(input, head, maxCats, maxLength)
      headProfile union profileStrAttrsByGroup(input, tail, maxCats, maxLength)
    }
  }

  private def profileNumAttrs(spark: SparkSession, totalCnt: Long, input: DataFrame,
                              maxDiscrete: Int, numBuckets: Int, minBucketSize: Int, randomSeed: Long): DataFrame = {
    val rdd: RDD[Row] = spark.sparkContext.parallelize(input.schema.map(field => {
      val numAttr = field.name
      val catCnts = input.groupBy(numAttr).count().limit(maxDiscrete + 1).filter(col(numAttr).isNotNull).collect()
      val nCats = catCnts.length
      val bkgAlgo = if (nCats <= maxDiscrete) {
        val profile = field.dataType match {
          case IntegerType => catCnts.map(row => (row.getInt(0), row.getLong(1))).sortBy(t => t._1).toList
          case LongType => catCnts.map(row => (row.getLong(0), row.getLong(1))).sortBy(t => t._1).toList
          case FloatType => catCnts.map(row => (row.getFloat(0), row.getLong(1))).sortBy(t => t._1).toList
          case DoubleType => catCnts.map(row => (row.getDouble(0), row.getLong(1))).sortBy(t => t._1).toList
          case _ => throw new UnsupportedOperationException(s"Non numeric type ${field.dataType}")
        }
        val bkt = new DiscreteBucket
        bkt.setValues(profile.map(_._1).asJava.asInstanceOf[java.util.List[Number]])
        bkt.setCounts(profile.map(_._2).asInstanceOf[List[java.lang.Long]].asJava)
        serializeJson(bkt)
      } else {
        val slice = input.select(numAttr).filter(col(numAttr).isNotNull).persist(StorageLevel.MEMORY_AND_DISK)
        val sliceCnt = slice.count
        val fraction: Double = if (sliceCnt > 100000) 100000.doubleValue / sliceCnt.doubleValue else 1.0
        val samples = slice.sample(fraction, randomSeed).collect()
        val values = field.dataType match {
          case IntegerType => samples.map(row => row.getInt(0))
          case LongType => samples.map(row => row.getLong(0))
          case FloatType => samples.map(row => row.getFloat(0))
          case DoubleType => samples.map(row => row.getDouble(0))
          case _ => throw new UnsupportedOperationException(s"Non numeric type ${field.dataType}")
        }
        val profiler = new NumericProfiler(values, numBuckets, minBucketSize, randomSeed)
        val bnds = profiler.findBoundaries()
        slice.unpersist()
        val bkt = new IntervalBucket
        field.dataType match {
          case IntegerType => bkt.setBoundaries(bnds.map(_.toInt).asJava.asInstanceOf[java.util.List[Number]])
          case LongType => bkt.setBoundaries(bnds.map(_.toLong).asJava.asInstanceOf[java.util.List[Number]])
          case FloatType => bkt.setBoundaries(bnds.map(_.toFloat).asJava.asInstanceOf[java.util.List[Number]])
          case DoubleType => bkt.setBoundaries(bnds.asJava.asInstanceOf[java.util.List[Number]])
          case _ => throw new UnsupportedOperationException(s"Non numeric type ${field.dataType}")
        }
        serializeJson(bkt)
      }
      Row.fromSeq(Seq(numAttr, bkgAlgo))
    }))
    val outputSchema = StructType(List(
      StructField(ATTR_ATTRNAME, StringType),
      StructField(ATTR_BKTALGO, StringType)
    ))
    spark.createDataFrame(rdd, outputSchema).persist(StorageLevel.MEMORY_AND_DISK)
  }

  private def profileNumAttrsByGroup(input: DataFrame, numAttrs: Seq[String], maxDiscrete: Int, numBuckets: Int,
                                     minBucketSize: Int, randomSeed: Long): DataFrame = {
    if (numAttrs.length <= NUM_CHUNK_SIZE) {
      val fields = input.schema.fields.filter(f => numAttrs.contains(f.name))
      val aggregate = new NumberProfileAggregation(fields, maxDiscrete, numBuckets, minBucketSize, randomSeed)
      input.agg(aggregate(numAttrs map col: _*).as("agg"))
        .withColumn("agg", explode(col("agg")))
        .withColumn(ATTR_ATTRNAME, col("agg.attr"))
        .withColumn(ATTR_BKTALGO, col("agg.algo"))
        .drop("agg")
    } else {
      val (head, tail) = numAttrs.splitAt(NUM_CHUNK_SIZE)
      val headProfile: DataFrame = profileNumAttrsByGroup(input, head, maxDiscrete, numBuckets, minBucketSize, randomSeed)
      headProfile union profileNumAttrsByGroup(input, tail, maxDiscrete, numBuckets, minBucketSize, randomSeed)
    }
  }

}
