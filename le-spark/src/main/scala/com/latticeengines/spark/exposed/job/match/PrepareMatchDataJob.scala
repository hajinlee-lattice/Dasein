package com.latticeengines.spark.exposed.job.`match`

import com.latticeengines.domain.exposed.serviceflows.core.spark.PrepareMatchDataJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.commons.collections4.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.JavaConverters._

/**
  * keep match fields and only one row per group (if provided)
  *
  * inputs: [ pre match input data ]
  * outputs: [ match input data ready for matching ]
  */
class PrepareMatchDataJob extends AbstractSparkJob[PrepareMatchDataJobConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[PrepareMatchDataJobConfig]): Unit = {
    // prepare input/vars
    val preMatch: DataFrame = lattice.input.head
    val config: PrepareMatchDataJobConfig = lattice.config
    val colsToRetain: Seq[Column] = if (CollectionUtils.isEmpty(config.matchFields)) {
      Nil
    } else {
      preMatch.columns.intersect(config.matchFields.asScala.toList) map col
    }

    // calculation
    var result = preMatch.select(colsToRetain:_*)
    if (StringUtils.isNotBlank(config.matchGroupId) && colsToRetain.contains(col(config.matchGroupId))) {
      result = result.dropDuplicates(Seq(config.matchGroupId))
    }

    lattice.output = result::Nil
  }
}
