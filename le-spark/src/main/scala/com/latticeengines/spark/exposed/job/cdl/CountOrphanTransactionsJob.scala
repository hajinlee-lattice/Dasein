package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.spark.cdl.CountOrphanTransactionsConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

class CountOrphanTransactionsJob extends AbstractSparkJob[CountOrphanTransactionsConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[CountOrphanTransactionsConfig]): Unit = {
    val config: CountOrphanTransactionsConfig = lattice.config
    val listSize = lattice.input.size
    val joinKeys = config.getJoinKeys.asScala.toList

    if (listSize == 3) {
      val transactionTable: DataFrame = lattice.input(0)
      val accountTable: DataFrame = lattice.input(1)
      val productTable: DataFrame = lattice.input(2)
      val accountJoinKey = joinKeys(0)
      val productJoinKey = joinKeys(1)
      val joinResult: DataFrame = transactionTable.join(accountTable, transactionTable(accountJoinKey) === accountTable(accountJoinKey), "left")
        .join(productTable, transactionTable(productJoinKey) === productTable(productJoinKey), "left").filter(accountTable(accountJoinKey).isNull || productTable(productJoinKey).isNull)
      lattice.outputStr = joinResult.count().toString
    }
  }
}
