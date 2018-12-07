package com.latticeengines.spark.job

import com.latticeengines.domain.exposed.spark.TestJoinJobConfig
import com.latticeengines.spark.exposed.job.AbstractSparkJob
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.{DataFrame, SparkSession}

class TestJoinJob extends AbstractSparkJob[TestJoinJobConfig] {

  override val name = "testJoin"

  override def runJob(spark: SparkSession, stageInput: List[DataFrame]): (Map[Integer, DataFrame], String) = {
    // read input
    val table1: DataFrame = stageInput(0)
    val table2: DataFrame = stageInput(1)

    // calculation
    val joinKey = "Field1"
    val aggKey = "Field2"
    val df = table1.join(table2, joinKey::Nil, "outer").groupBy(joinKey)
    val out1 = df.count().withColumnRenamed("count", "Cnt")
    val out2 = df.agg(max(table1(aggKey)).as("Max1"), max(table2(aggKey)).as("Max2"))

    // finish
    val output: Map[Integer, DataFrame] = Map((0, out1), (1, out2))
    (output, "This is my output!")
  }

}
