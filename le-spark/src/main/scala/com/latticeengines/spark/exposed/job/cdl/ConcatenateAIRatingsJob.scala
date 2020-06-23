package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.spark.cdl.ConcatenateAIRatingsConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.MergeUtils
import org.apache.spark.sql.SparkSession

class ConcatenateAIRatingsJob extends AbstractSparkJob[ConcatenateAIRatingsConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[ConcatenateAIRatingsConfig]): Unit = {
    val result = lattice.input.reduce((l, r) => {
      MergeUtils.concat2(l, r)
    })
    lattice.output = List(result)
  }

}
