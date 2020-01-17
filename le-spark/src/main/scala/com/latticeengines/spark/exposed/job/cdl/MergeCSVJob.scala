package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit
import com.latticeengines.domain.exposed.spark.cdl.MergeCSVConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.CSVUtils
import org.apache.spark.sql.SparkSession

class MergeCSVJob extends AbstractSparkJob[MergeCSVConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[MergeCSVConfig]): Unit = {
    val input = lattice.input.head
    lattice.output = input :: Nil
  }

  override def finalizeJob(spark: SparkSession, latticeCtx: LatticeContext[MergeCSVConfig]): List[HdfsDataUnit] = {
    CSVUtils.dfToCSV(spark, false, latticeCtx.targets, latticeCtx.output)
  }
}
