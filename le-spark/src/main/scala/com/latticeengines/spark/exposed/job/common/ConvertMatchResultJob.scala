package com.latticeengines.spark.exposed.job.common

import com.latticeengines.domain.exposed.spark.common.ConvertMatchResultConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.DisplayNameUtils
import org.apache.spark.sql.SparkSession

class ConvertMatchResultJob extends AbstractSparkJob[ConvertMatchResultConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[ConvertMatchResultConfig]): Unit = {
    val config: ConvertMatchResultConfig = lattice.config
    var input = lattice.input.head
    input = DisplayNameUtils.changeToDisplayName(input, config.getDisplayNames)
    lattice.output = input :: Nil
  }

}
