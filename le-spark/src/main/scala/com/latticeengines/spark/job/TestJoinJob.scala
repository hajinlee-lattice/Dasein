package com.latticeengines.spark.job

import com.latticeengines.domain.exposed.spark.TestJoinJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.{DataFrame, SparkSession}

class TestJoinJob extends AbstractSparkJob[TestJoinJobConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[TestJoinJobConfig]): Unit = {
    // read input
    val table1: DataFrame = lattice.input.head
    val table2: DataFrame = lattice.input(1)

    // calculation
    val joinKey = "Field1"
    val aggKey = "Field2"
    val df = table1.join(table2, joinKey::Nil, "outer").groupBy(joinKey)
    val out1 = df.count().withColumnRenamed("count", "Cnt")
    val out2 = df.agg(max(table1(aggKey)).as("Max1"), max(table2(aggKey)).as("Max2"))

    logSpark("Here is my log")

    // finish
    lattice.output = out1::out2::Nil
    lattice.outputStr = "This is my output!"
  }
}
