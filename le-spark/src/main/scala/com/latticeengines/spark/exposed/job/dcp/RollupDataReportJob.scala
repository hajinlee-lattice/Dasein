package com.latticeengines.spark.exposed.job.dcp

import com.latticeengines.domain.exposed.spark.dcp.RollupDataReportConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.SparkSession

class RollupDataReportJob extends AbstractSparkJob[RollupDataReportConfig]{
  override def runJob(spark: SparkSession, lattice: LatticeContext[RollupDataReportConfig]): Unit = {

  }
}
