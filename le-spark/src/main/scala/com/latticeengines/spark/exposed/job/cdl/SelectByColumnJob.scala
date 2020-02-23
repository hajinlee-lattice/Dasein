package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.spark.cdl.SelectByColumnConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

class SelectByColumnJob extends AbstractSparkJob[SelectByColumnConfig] {
  override def runJob(spark: SparkSession, lattice: LatticeContext[SelectByColumnConfig]): Unit = {
    val config: SelectByColumnConfig = lattice.config
    val joinColumn = config.getSourceColumn
    val destColumn = config.getDestColumn

    val source: DataFrame = lattice.input.head
    val original: DataFrame = lattice.input(1)

    val result = original.join(source, Seq(joinColumn), "inner").select(destColumn).distinct()
    lattice.output = result::Nil


  }
}
