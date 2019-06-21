package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.spark.cdl.MergeRuleRatingsConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

class MergeRuleRatings extends AbstractSparkJob[MergeRuleRatingsConfig] {

  private val ACCOUNT_ID = InterfaceName.AccountId.name
  private val RATING = InterfaceName.Rating.name

  override def runJob(spark: SparkSession, lattice: LatticeContext[MergeRuleRatingsConfig]): Unit = {
    val config: MergeRuleRatingsConfig = lattice.config
    val inputDfs = lattice.input
    val defaultBkt = config.getDefaultBucketName

    val defaultBktDf = inputDfs.head.select(ACCOUNT_ID)

    val result: DataFrame = if (inputDfs.length > 1) {
      val bktDfs = inputDfs.tail map (df => df.select(ACCOUNT_ID))
      val bktNames = config.getBucketNames.asScala.toList
      mergeRatings(defaultBktDf, bktDfs, bktNames, defaultBkt)
    } else {
      defaultBktDf.withColumn(RATING, lit(defaultBkt))
    }

    // finish
    lattice.output = result::Nil
  }

  private def mergeRatings(defaultBkt: DataFrame, bktDfs: Seq[DataFrame], bktNames: Seq[String], //
                           defaultBktName: String): DataFrame = {
    if (bktDfs.length != bktNames.length) {
      throw new IllegalArgumentException("There are " + bktDfs.length + " data frames " + //
        "but " + bktNames.length + " bucket names.")
    }

    val scored = (bktDfs zip bktNames).map(t => {
      val (df, bkt) = t
      df.withColumn(RATING, lit(bkt))
    })

    val merged = scored.reduce(_ union _).groupBy(ACCOUNT_ID).agg(min(RATING).alias(RATING))

    defaultBkt.join(merged, Seq(ACCOUNT_ID), joinType = "left")
      .withColumn(RATING, when(col(RATING).isNull, lit(defaultBktName)).otherwise(col(RATING)))
  }

}
