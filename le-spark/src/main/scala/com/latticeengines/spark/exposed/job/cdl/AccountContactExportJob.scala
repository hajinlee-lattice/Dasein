package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.pls.AccountContactExportContext
import com.latticeengines.domain.exposed.spark.cdl.AccountContactExportConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

class AccountContactExportJob extends AbstractSparkJob[AccountContactExportConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[AccountContactExportConfig]): Unit = {
    val config: AccountContactExportConfig = lattice.config
    val accountContactExportContext: AccountContactExportContext = config.getAccountContactExportContext
    // Read Input
    val listSize = lattice.input.size
    println(s"input size is: $listSize")
    val accountTable: DataFrame = lattice.input.head
    val joinKey: String = accountContactExportContext.getJoinKey
    println("----- BEGIN SCRIPT OUTPUT -----")
    println(s"joinKey is: $joinKey")
    println("----- END SCRIPT OUTPUT -----")

    if (listSize == 2) {
      val contactTable: DataFrame = lattice.input(1)
      // left join
      var joinResult = accountTable.join(contactTable, joinKey :: Nil, "left")
      if (config.getDropKeys != null) {
        config.getDropKeys.asScala.foreach(dropKey => joinResult = joinResult.drop(dropKey))
      }
      lattice.output = joinResult :: Nil
    }
  }
}
