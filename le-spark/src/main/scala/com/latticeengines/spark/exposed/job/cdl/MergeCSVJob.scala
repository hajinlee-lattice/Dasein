package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit
import com.latticeengines.domain.exposed.spark.cdl.MergeCSVConfig
import com.latticeengines.spark.exposed.job.LatticeContext
import com.latticeengines.spark.exposed.job.common.CSVJobBase
import org.apache.spark.sql.SparkSession

class MergeCSVJob extends CSVJobBase[MergeCSVConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[MergeCSVConfig]): Unit = {
    val input = lattice.input.head
    val config: MergeCSVConfig = lattice.config
    val result = customizeField(input, config)
    lattice.output = result :: Nil
  }

  override def finalizeJob(spark: SparkSession, latticeCtx: LatticeContext[MergeCSVConfig]): List[HdfsDataUnit] = {
    dfToCSV(spark, latticeCtx.config, latticeCtx.targets, latticeCtx.output)
  }
}
