package com.latticeengines.spark.exposed.job.common

import com.latticeengines.domain.exposed.spark.common.CopyConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.CopyUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Concatenate inputs into a single dataset
  * Then write out to one output
  * Can perform simple column selection and rename
  * Operation sequence: select -> drop -> rename -> add create/update timestamps
  */
class CopyJob extends AbstractSparkJob[CopyConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[CopyConfig]): Unit = {
    val config: CopyConfig = lattice.config
    val inputs: List[DataFrame] = lattice.input
    val concatenated = CopyUtils.copy(config, inputs)
    lattice.output = concatenated :: Nil
  }

}
