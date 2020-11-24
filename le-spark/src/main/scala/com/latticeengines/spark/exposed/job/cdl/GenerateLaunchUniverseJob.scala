package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.spark.cdl.GenerateLaunchUniverseJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


class GenerateLaunchUniverseJob extends AbstractSparkJob[GenerateLaunchUniverseJobConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[GenerateLaunchUniverseJobConfig]): Unit = {

    val config: GenerateLaunchUniverseJobConfig = lattice.config
    val input : DataFrame = lattice.input.head
    val accountId = InterfaceName.AccountId.name()
    val maxContactsPerAccount = config.getMaxContactsPerAccount
    val maxEntitiesToLaunch = config.getMaxEntitiesToLaunch
    val sortAttr = config.getContactsPerAccountSortAttribute
    val sortDir = config.getContactsPerAccountSortDirection

    logSpark("Input schema is as follows:")
    input.printSchema

    var trimmedData = input

    if (maxContactsPerAccount != null) {
      trimmedData = limitContactsPerAccount(trimmedData, accountId, sortAttr, sortDir, maxContactsPerAccount)
    }

    if (maxEntitiesToLaunch != null) {
      trimmedData = trimmedData.limit(maxEntitiesToLaunch.toInt)
    }

    lattice.output = List(trimmedData)
  }

  def limitContactsPerAccount(trimmedData: DataFrame, accountId: String, sortAttr: String, sortDir: String, maxContactsPerAccount: Long): DataFrame = {
    val rowNumber = "rowNumber"
    var w = Window.partitionBy(accountId)

    if (trimmedData.columns.contains(sortAttr)) {
      if (sortDir == "DESC") {
        w = w.orderBy(col(sortAttr).desc)
      } else {
        w = w.orderBy(col(sortAttr))
      }
    }

    trimmedData
      .withColumn(rowNumber, row_number.over(w))
      .filter(col(rowNumber) <= maxContactsPerAccount.toInt)
      .drop(rowNumber)
  }
}
