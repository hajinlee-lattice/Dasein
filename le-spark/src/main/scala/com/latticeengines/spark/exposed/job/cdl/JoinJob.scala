package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.spark.cdl.JoinConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

class JoinJob extends AbstractSparkJob[JoinConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[JoinConfig]): Unit = {
    val config: JoinConfig = lattice.config
    val joinKey: String = config.getJoinKey
	println(s"joinKey is: $joinKey");
	
	// read input
    val accountTable: DataFrame = lattice.input.head
    val contactTable: DataFrame = lattice.input(1)

    // join
    val df = accountTable.join(contactTable, joinKey::Nil, "left").groupBy(joinKey)
    val out1 = df.count().withColumnRenamed("count", "Cnt")
	
    // finish
    lattice.output = out1::Nil
    lattice.outputStr = "This is my recommendation!"
  }

}
