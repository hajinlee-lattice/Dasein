package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.pls.AccountContactExportContext
import com.latticeengines.domain.exposed.spark.cdl.AccountContactExportConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

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
      var contactTable: DataFrame = lattice.input(1)
      val newAttrs = contactTable.columns.map(c => AccountContactExportConfig.CONTACT_ATTR_PREFIX + c)
      contactTable = contactTable.toDF(newAttrs: _*)
      // left join
      val contactJoinKey = AccountContactExportConfig.CONTACT_ATTR_PREFIX + joinKey;
      var joinResult: DataFrame = accountTable.join(contactTable, accountTable(joinKey) === contactTable(contactJoinKey), "left")
      lattice.output = joinResult :: Nil
    }
  }
}
