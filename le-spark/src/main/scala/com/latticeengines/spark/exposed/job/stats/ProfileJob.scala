package com.latticeengines.spark.exposed.job.stats

import com.latticeengines.domain.exposed.datacloud.DataCloudConstants
import com.latticeengines.domain.exposed.datacloud.dataflow.stats.ProfileParameters.Attribute
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook
import com.latticeengines.domain.exposed.spark.stats.ProfileJobConfig
import com.latticeengines.spark.aggregation.{NumberProfileAggregation, StringProfileAggregation}
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.BitEncodeUtils
import org.apache.spark.sql.functions.{col, explode, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

class ProfileJob extends AbstractSparkJob[ProfileJobConfig] {

  private val STR_CHUNK_SIZE: Int = 1000
  private val NUM_CHUNK_SIZE: Int = 100
  private val ATTR_DECSTRAT = DataCloudConstants.PROFILE_ATTR_DECSTRAT
  private val ATTR_ATTRNAME = DataCloudConstants.PROFILE_ATTR_ATTRNAME
  private val ATTR_BKTALGO = DataCloudConstants.PROFILE_ATTR_BKTALGO

  private val NUM_TYPES: Set[DataType] = Set(IntegerType, LongType, FloatType, DoubleType)

  override def runJob(spark: SparkSession, lattice: LatticeContext[ProfileJobConfig]): Unit = {
    val inputData: DataFrame = lattice.input.head
    val config: ProfileJobConfig = lattice.config
    val numAttrs = getNumAttrs(inputData, config)
    val randomSeed: Long = if (config.getRandSeed == null) System.currentTimeMillis else config.getRandSeed
    val numProfile = profileNumAttrs(numAttrs, numAttrs.columns.toList, config.getMaxDiscrete, config.getBucketNum, config.getMinBucketSize, randomSeed)

    val catAttrs: List[String] =
      if (config.getCatAttrs == null) List() else config.getCatAttrs.asScala.map(_.getAttr).toList
    val strProfile = profileStrAttrs(inputData, catAttrs, config.getMaxCat, config.getMaxCatLength)

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

    val output = BitEncodeUtils.decode(input, numAttrs, codeBookLookup, codeBookMap)
    output.schema.foreach(field => {
      if (!NUM_TYPES.contains(field.dataType)) {
        throw new IllegalArgumentException(s"Attribute ${field.name} of type ${field.dataType} is not numerical.")
      }
    })
    output
  }

  private def profileStrAttrs(input: DataFrame, catAttrs: List[String], maxCats: Int, maxLength: Int): DataFrame = {
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
      val headProfile: DataFrame = profileStrAttrs(input, head, maxCats, maxLength)
      headProfile union profileStrAttrs(input, tail, maxCats, maxLength)
    }
  }

  private def profileNumAttrs(input: DataFrame, cols: List[String], maxDiscrete: Int, numBuckets: Int, minBucketSize: Int, randomSeed: Long): DataFrame = {
    if (cols.length <= NUM_CHUNK_SIZE) {
      val fields = input.schema.fields.filter(f => cols.contains(f.name))
      val aggregate = new NumberProfileAggregation(fields, maxDiscrete, numBuckets, minBucketSize, randomSeed)
      input.agg(aggregate(cols map col: _*).as("agg"))
        .withColumn("agg", explode(col("agg")))
        .withColumn(ATTR_ATTRNAME, col("agg.attr"))
        .withColumn(ATTR_BKTALGO, col("agg.algo"))
        .drop("agg")
    } else {
      val (head, tail) = cols.splitAt(NUM_CHUNK_SIZE)
      val headProfile: DataFrame = profileNumAttrs(input, head, maxDiscrete, numBuckets, minBucketSize, randomSeed)
      headProfile union profileNumAttrs(input, tail, maxDiscrete, numBuckets, minBucketSize, randomSeed)
    }
  }

}
