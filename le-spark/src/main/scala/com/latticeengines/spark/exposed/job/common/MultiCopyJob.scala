package com.latticeengines.spark.exposed.job.common

import com.latticeengines.domain.exposed.spark.common.{CopyConfig, MultiCopyConfig}
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.CopyUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

/**
  * Similar to CopyJob, but write to more than one output
  * Operation sequence: select -> drop -> rename -> add create/update timestamps
  */
class MultiCopyJob extends AbstractSparkJob[MultiCopyConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[MultiCopyConfig]): Unit = {
    val inputs: List[DataFrame] = lattice.input
    val copyConfigs: Seq[CopyConfig] = lattice.config.getCopyConfigs.asScala
    lattice.output = copyConfigs.map(copyConfig => {
      CopyUtils.copy(copyConfig, inputs)
    }).toList
  }

}
