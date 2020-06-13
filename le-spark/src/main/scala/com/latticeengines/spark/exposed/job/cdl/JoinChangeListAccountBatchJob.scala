package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.spark.cdl.JoinChangeListAccountBatchConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

class JoinChangeListAccountBatchJob extends AbstractSparkJob[JoinChangeListAccountBatchConfig] {
  override def runJob(spark: SparkSession, lattice: LatticeContext[JoinChangeListAccountBatchConfig]): Unit = {
    val config: JoinChangeListAccountBatchConfig = lattice.config
    val joinKey = config.getJoinKey
    val selectColumns = config.getSelectColumns.asScala.toSeq
    val changelist: DataFrame = lattice.input.head
    val accountbatch: DataFrame = lattice.input(1)

    // Rename changelist column "RowId" to the joinKey
    val renamed = changelist.withColumnRenamed("RowId", joinKey)
    val result = accountbatch.join(renamed, Seq(joinKey), "inner").select(selectColumns map col: _*).distinct()
    lattice.output = result :: Nil
  }
}
