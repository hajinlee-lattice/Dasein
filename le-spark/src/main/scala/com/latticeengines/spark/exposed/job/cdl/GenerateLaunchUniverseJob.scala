package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit
import com.latticeengines.domain.exposed.spark.cdl.GenerateLaunchUniverseJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.{Row, SparkSession}


class GenerateLaunchUniverseJob extends AbstractSparkJob[GenerateLaunchUniverseJobConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[GenerateLaunchUniverseJobConfig]): Unit = {

    val config: GenerateLaunchUniverseJobConfig = lattice.config
    val launchData = loadHdfsUnit(spark, config.getLaunchData.asInstanceOf[HdfsDataUnit])
    val accountId = InterfaceName.AccountId.name()
    val maxContactsPerAccount = config.getMaxContactsPerAccount
    val maxAccountsToLaunch = config.getMaxAccountsToLaunch
    val sortAttr = config.getContactsPerAccountSortAttribute
    val sortDir = config.getContactsPerAccountSortDirection

    logSpark("launchData schema is as follows:")
    launchData.printSchema

    var trimmedData = launchData
    var w = Window.partitionBy(accountId)

    if (maxContactsPerAccount != null) {
      val rowNumber = "rowNumber"

      if (sortDir == "DESC") {
        w = w.orderBy(col(sortAttr).desc)
      } else {
        w = w.orderBy(col(sortAttr))
      }

      trimmedData = launchData
        .withColumn(rowNumber, row_number.over(w))
        .filter(col(rowNumber) <= maxContactsPerAccount)
        .drop(rowNumber)
    }

    if (maxAccountsToLaunch != null) {
      trimmedData = trimmedData.limit(maxAccountsToLaunch.toInt)
    }

    lattice.output = List(trimmedData)
  }

}
