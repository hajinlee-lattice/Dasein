package com.latticeengines.spark.exposed.job.common

import com.latticeengines.domain.exposed.spark.common.UpsertConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.MergeUtils
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

class UpsertJob extends AbstractSparkJob[UpsertConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[UpsertConfig]): Unit = {
    val config: UpsertConfig = lattice.config
    val lhsDf = lattice.input.head
    val rhsDf = lattice.input(1)

    val joinKey = config.getJoinKey
    val colsFromLhs: Set[String] = if (config.getColsFromLhs == null) Set() else config.getColsFromLhs.asScala.toSet
    val overwriteByNull: Boolean =
      if (config.getNotOverwriteByNull == null) true else !config.getNotOverwriteByNull.booleanValue()

    val merged = MergeUtils.merge2(lhsDf, rhsDf, Seq(joinKey), colsFromLhs, overwriteByNull = overwriteByNull)

    // finish
    lattice.output = merged :: Nil
  }

}
