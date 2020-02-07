package com.latticeengines.spark.job

import com.latticeengines.domain.exposed.spark.SparkJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.MergeUtils
import org.apache.spark.sql.SparkSession

class TestConcatJob extends AbstractSparkJob[SparkJobConfig] {
  override def runJob(spark: SparkSession, lattice: LatticeContext[SparkJobConfig]): Unit = {
    assert(lattice.input.length == 2, "Should have two inputs")
    lattice.output = MergeUtils.concat2(lattice.input.head, lattice.input(1)) :: Nil
  }
}
