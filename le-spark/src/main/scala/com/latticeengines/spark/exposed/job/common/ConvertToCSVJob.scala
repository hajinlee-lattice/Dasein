package com.latticeengines.spark.exposed.job.common

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit
import com.latticeengines.domain.exposed.spark.common.ConvertToCSVConfig
import com.latticeengines.spark.exposed.job.LatticeContext
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

class ConvertToCSVJob extends CSVJobBase[ConvertToCSVConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[ConvertToCSVConfig]): Unit = {
    val config: ConvertToCSVConfig = lattice.config
    val input = lattice.input.head
    val withTimestamp = addTimestamp(input, config)
    val result = customizeField(withTimestamp, config)
    lattice.output = result :: Nil
  }

  private def addTimestamp(input: DataFrame, config: ConvertToCSVConfig): DataFrame = {
    val timeAttr = config.getExportTimeAttr
    if (StringUtils.isNotBlank(timeAttr)) {
      val now = System.currentTimeMillis
      input.withColumn(timeAttr, lit(now))
    } else {
      input
    }
  }

  override def finalizeJob(spark: SparkSession, latticeCtx: LatticeContext[ConvertToCSVConfig]): List[HdfsDataUnit] = {
    dfToCSV(spark, latticeCtx.config, latticeCtx.targets, latticeCtx.output)
  }

}
