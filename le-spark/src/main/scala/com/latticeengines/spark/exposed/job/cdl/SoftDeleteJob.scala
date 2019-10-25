package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.spark.cdl.{RemoveOrphanConfig, SoftDeleteConfig}
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

class SoftDeleteJob extends AbstractSparkJob[SoftDeleteConfig] {
  override def runJob(spark: SparkSession, lattice: LatticeContext[SoftDeleteConfig]): Unit = {
    val config: SoftDeleteConfig = lattice.config
    val deleteSrcIdx: Int = if (config.getDeleteSourceIdx == null) 1 else config.getDeleteSourceIdx.toInt
    val originalSrcIdx: Int = (deleteSrcIdx + 1) % 2
    val joinColumn = config.getIdColumn

    val delete: DataFrame = lattice.input(deleteSrcIdx)
    val original: DataFrame = lattice.input(originalSrcIdx)

    // calculation
    val result = original.alias("original")
      .join(delete, Seq(joinColumn), "left")
      .where(delete.col(joinColumn).isNull)
      .select("original.*")

    // finish
    lattice.output = result::Nil
  }
}
