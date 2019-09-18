package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.pls.AccountContactExportContext
import com.latticeengines.domain.exposed.spark.cdl.AccountContactExportConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.commons.collections4.{CollectionUtils, MapUtils}
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
      var contactTable: DataFrame = lattice.input(1)
      contactTable = renameAccountTableColumn(contactTable, config)
      // left join
      val contactJoinKey = getContactJoinKey(joinKey, config)
      var joinResult = accountTable.join(contactTable, accountTable(joinKey) === contactTable(contactJoinKey), "left")
      if (CollectionUtils.isNotEmpty(config.getDropKeys)) {
        config.getDropKeys.asScala.foreach(dropKey => joinResult = joinResult.drop(dropKey))
      }
      lattice.output = joinResult :: Nil
    }
  }

  private def getContactJoinKey(joinKey: String, config: AccountContactExportConfig): String = {
    if (MapUtils.isEmpty(config.getContactColumnNames)) {
      joinKey
    } else {
      config.getContactColumnNames.asScala.toMap.getOrElse(joinKey, joinKey)
    }
  }

  private def renameAccountTableColumn(contactTable: DataFrame, config: AccountContactExportConfig): DataFrame = {
    if (MapUtils.isEmpty(config.getContactColumnNames)) {
      contactTable
    } else {
      val columnsToRename: Map[String, String] = config.getContactColumnNames.asScala.toMap
        .filterKeys(contactTable.columns.contains(_))
      val newAttrs = contactTable.columns.map(c => columnsToRename.getOrElse(c, c))
      contactTable.toDF(newAttrs: _*)
    }
  }
}
