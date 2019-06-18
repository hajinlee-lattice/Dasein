package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.spark.cdl.RemoveOrphanConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

class RemoveOrphanJob extends AbstractSparkJob[RemoveOrphanConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[RemoveOrphanConfig]): Unit = {
    val config: RemoveOrphanConfig = lattice.config
    val parentSrcIdx: Int = if (config.getParentSrcIdx == null) 1 else config.getParentSrcIdx.toInt
    val childSrcIdx: Int = (parentSrcIdx + 1) % 2
    val parentId = config.getParentId

    val parent: DataFrame = lattice.input(parentSrcIdx)
    val child: DataFrame = lattice.input(childSrcIdx)

    // calculation
    val result = parent.select(parentId).join(child, Seq(parentId), "inner")

    // finish
    lattice.output = result::Nil
  }

}
