package com.latticeengines.spark.exposed.job.cm

import com.latticeengines.domain.exposed.spark.cm.CMTpsLookupCreationConfig
import com.latticeengines.spark.aggregation.ConcatStringsUDAF
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class CreateCMTpsLookupJob extends AbstractSparkJob[CMTpsLookupCreationConfig] {
  override def runJob(spark: SparkSession, lattice: LatticeContext[CMTpsLookupCreationConfig]): Unit = {
    val config: CMTpsLookupCreationConfig = lattice.config
    val tpsSource: DataFrame = lattice.input(0)
    val key = config.getKey
    val targetColumn = config.getTargetColumn

    // Filter out rows which has null keys
    val filtered = tpsSource.filter(col(key).isNotNull)

    val concatUdaf = new ConcatStringsUDAF(targetColumn, ",")
    val result = filtered.groupBy(key).agg(concatUdaf(col(targetColumn)).as("RECORD_IDS"))

    lattice.output = result :: Nil
  }
}
