package com.latticeengines.spark.exposed.job.common

import com.latticeengines.domain.exposed.spark.common.ConvertMatchResultConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.commons.collections4.MapUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

class ConvertMatchResultJob extends AbstractSparkJob[ConvertMatchResultConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[ConvertMatchResultConfig]): Unit = {
    val config: ConvertMatchResultConfig = lattice.config
    var input = lattice.input.head
    input = changeToDisplayName(input, config)
    lattice.output = input :: Nil
  }

  private def changeToDisplayName(input: DataFrame, convertMatchResultConfig: ConvertMatchResultConfig): DataFrame = {
    if (MapUtils.isEmpty(convertMatchResultConfig.getDisplayNames)) {
      input
    } else {
      val attrsToRename: Map[String, String] = convertMatchResultConfig.getDisplayNames.asScala.toMap
        .filterKeys(input.columns.contains(_))
      val newAttrs = input.columns.map(c => attrsToRename.getOrElse(c, c))
      input.toDF(newAttrs: _*)
    }
  }
}
