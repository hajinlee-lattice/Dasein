package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.camille.CustomerSpace
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection.TimelineProfile
import com.latticeengines.domain.exposed.metadata.{InterfaceName, TableRoleInCollection}
import com.latticeengines.domain.exposed.query.BusinessEntity
import com.latticeengines.domain.exposed.spark.cdl.ExportToElasticSearchJobConfig
import com.latticeengines.domain.exposed.util.TimeLineStoreUtils.TimelineStandardColumn
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.MergeUtils
import org.apache.spark.sql.functions.{collect_list, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql._

import scala.collection.JavaConverters.mapAsScalaMapConverter

class ExportToElasticSearchJob extends AbstractSparkJob[ExportToElasticSearchJobConfig] {

  private val entityId = InterfaceName.EntityId.name
  private val accountId = InterfaceName.AccountId.name
  private val contactId = InterfaceName.ContactId.name
  private val lookupKey = InterfaceName.AtlasLookupKey.name
  private val recordId = TimelineStandardColumn.RecordId.getColumnName

  override def runJob(spark: SparkSession, lattice: LatticeContext[ExportToElasticSearchJobConfig]): Unit = {
    //define var
    val config: ExportToElasticSearchJobConfig = lattice.config
    val esConfig = config.esConfig
    val inputIdx = config.inputIdx.asScala
    val entityWithESVersionMap = config.entityWithESVersionMap.asScala
    val customerSpace = config.customerSpace
    val lookupTable: DataFrame =
      if (config.lookupIdx != null) {
        lattice.input(config.lookupIdx)
      } else {
        null
      }

    val baseConfig = Map(
      "es.write.operation" -> "upsert",
      "es.nodes.wan.only" -> "true",
      "es.index.auto.create" -> "false",
      "es.batch.write.refresh" -> "false",
      //      "es.batch.size.bytes" -> "10mb",
      "es.nodes" -> esConfig.getEsHost,
      "es.port" -> esConfig.getEsPort,
      "es.net.http.auth.user" -> "root",
      "es.net.http.auth.pass" -> "9Nc-CX$?",
      "es.net.ssl" -> "true")

    var accountDf: DataFrame = null
    var contactDf: DataFrame = null
    inputIdx.foreach { case (tableRoleInCollection, idxList) =>
      val start = idxList.get(0).intValue()
      val end = idxList.get(1).intValue()
      if (tableRoleInCollection.contains("Account") || tableRoleInCollection.contains("PurchaseHistory") || tableRoleInCollection.contains("Rating")) {

        if (lookupTable != null && TableRoleInCollection.AccountLookup.name().eq(tableRoleInCollection)) {
          val groupTable = lookupTable.groupBy(accountId).agg(concat_ws(",", collect_list(lookupKey)) as
            "AtlasLookupKeys")
          val finalTable = groupTable.withColumnRenamed("AtlasLookupKeys", lookupKey)

          accountDf = merge(addPrefix(finalTable, tableRoleInCollection), accountDf, accountId)
        } else {
          for (i <- start until end) {
            val origin: DataFrame = lattice.input(i)
            accountDf = merge(addPrefix(origin, tableRoleInCollection), accountDf, accountId)
          }
        }
      } else if (tableRoleInCollection.contains("Contact")) {
        val entity = BusinessEntity.Contact.name
        val index = String
          .format("%s_%s_%s", CustomerSpace.shortenCustomerSpace(customerSpace), entity, entityWithESVersionMap(entity))
          .toLowerCase
        for (i <- start until end) {
          val origin: DataFrame = lattice.input(i)
          contactDf = merge(addPrefix(origin, tableRoleInCollection), contactDf, contactId)
        }
      } else if (TimelineProfile.name().eq(tableRoleInCollection)) {
        for (i <- start until end) {
          val origin: DataFrame = lattice.input(i)
          val index = String.format("%s_%s_%s",
            CustomerSpace.shortenCustomerSpace(customerSpace),
            TimelineProfile.name,
            entityWithESVersionMap(TimelineProfile.name))
            .toLowerCase
          origin.saveToEs(index, baseConfig + ("es.mapping.id" -> recordId))
        }
      }
    }
    if (accountDf != null) {
      val entity = BusinessEntity.Account.name
      val index = String
        .format("%s_%s_%s", CustomerSpace.shortenCustomerSpace(customerSpace), entity, entityWithESVersionMap(entity))
        .toLowerCase
      accountDf.saveToEs(index, baseConfig + ("es.mapping.id" -> accountId))
    }
    if (contactDf != null) {
      val entity = BusinessEntity.Contact.name
      val index = String
        .format("%s_%s_%s", CustomerSpace.shortenCustomerSpace(customerSpace), entity, entityWithESVersionMap(entity))
        .toLowerCase
      contactDf.saveToEs(index, baseConfig + ("es.mapping.id" -> contactId))
    }

  }

  private def merge(df: DataFrame, accDf: DataFrame, joinKeys: String): DataFrame = {
    if (accDf == null) {
      df
    } else {
      MergeUtils.merge2(accDf, df, Seq(joinKeys), Set(), overwriteByNull = false)
    }
  }

  private def addPrefix(df: DataFrame, prefix: String): DataFrame = {
    val role = TableRoleInCollection.getByName(prefix).ordinal.toString
    val cols = df.columns.toSeq
    df.select(cols.map(c => {
      if (c.equals(accountId) || c.equals(contactId) || c.equals(entityId)) {
        df.col(c)
      } else {
        df.col(c).as(role + ":" + c)
      }
    }): _*)
  }
}