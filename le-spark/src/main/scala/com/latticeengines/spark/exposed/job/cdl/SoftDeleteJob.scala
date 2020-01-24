package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.spark.cdl.SoftDeleteConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters.asScalaIteratorConverter

class SoftDeleteJob extends AbstractSparkJob[SoftDeleteConfig] {
  override def runJob(spark: SparkSession, lattice: LatticeContext[SoftDeleteConfig]): Unit = {
    val config: SoftDeleteConfig = lattice.config
    val deleteSrcIdx: Int = if (config.getDeleteSourceIdx == null) 1 else config.getDeleteSourceIdx.toInt
    val originalSrcIdx: Int = (deleteSrcIdx + 1) % 2
    val joinColumn = config.getIdColumn
    val hasPartitionKey = config.getPartitionKeys != null && config.getPartitionKeys.size() > 0

    if (hasPartitionKey) {
      setPartitionTargets(0, asScalaIteratorConverter(config.getPartitionKeys.iterator).asScala.toSeq, lattice)
    }

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
