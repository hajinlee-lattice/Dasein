package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.common.exposed.util.JsonUtils
import com.latticeengines.domain.exposed.camille.CustomerSpace
import com.latticeengines.domain.exposed.metadata.{InterfaceName, TableRoleInCollection}
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection._
import com.latticeengines.domain.exposed.query.BusinessEntity
import com.latticeengines.domain.exposed.spark.cdl.PublishTableToElasticSearchJobConfiguration
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.ElasticSearchUtils
import com.latticeengines.spark.util.ElasticSearchUtils._
import org.apache.spark.sql.functions.{col, first, lit, map, udf}
import org.apache.spark.sql.types.{StringType, StructField}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.elasticsearch.spark.sql._
import org.xerial.snappy.Snappy

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.Breaks._

class PublishTableToElasticSearchJob extends AbstractSparkJob[PublishTableToElasticSearchJobConfiguration]{

  override def runJob(spark: SparkSession, lattice: LatticeContext[PublishTableToElasticSearchJobConfiguration])
  : Unit = {
    val config = lattice.config
    val inputs = lattice.input
    val indexToRole = config.getIndexToRole.asScala
    val indexToSignature = config.getIndexToSignature.asScala
    val keys = indexToRole.keys.toSeq
    val customerSpace = config.getCustomerSpace
    val esConfigs = config.getEsConfigs
    val baseConfig = getBaseConfig(esConfigs.getEsHost, esConfigs.getEsPort, esConfigs.getEsUser, esConfigs
      .getEsPassword, esConfigs.getEncryptionKey, esConfigs.getSalt)

    for (key <- keys) {
      breakable {
        val table: DataFrame = inputs(key)
        val signature = indexToSignature(key)
        val role = indexToRole(key)
        val entity = com.latticeengines.elasticsearch.util.ElasticSearchUtils.getEntityFromTableRole(role)
        val docIdCol = if (BusinessEntity.Account.name().eq(entity)) {
          accountId
        } else if (BusinessEntity.Contact.name().eq(entity)) {
          contactId
        } else if (TimelineProfile.name().eq(entity)) {
          sortKey
        } else {
          null
        }

        if (entity == null || docIdCol == null) {
          logSpark(s"entity or doc id column is not provided $role")
          break
        }


        val indexName = ElasticSearchUtils.constructIndexName(CustomerSpace.shortenCustomerSpace(customerSpace), entity,
          signature)
        if (role == TableRoleInCollection.TimelineProfile)
          table.saveToEs(indexName, baseConfig + ("es.mapping.id" -> docIdCol))
        else if (role == TableRoleInCollection.AccountLookup)
          saveToESWithMeta(table, indexName, role, entity, docIdCol, baseConfig, false)
        else
          saveToESWithMeta(table, indexName, role, entity, docIdCol, baseConfig, true)
      }
    }
  }

  def saveToESWithMeta(table : DataFrame, indexName : String, role : TableRoleInCollection, entity : String,
                       docIdCol: String, baseConfig : Map[String, String], compressed : Boolean) : Unit = {

    if (compressed) {
      val compressUdf = udf((s: Map[String, String]) => Snappy.compress(JsonUtils.serialize(s.asJava)))
      val columns: mutable.LinkedHashSet[Column] = mutable.LinkedHashSet[Column]()
      table.schema.fields.foreach((field: StructField) => {
        columns.add(lit(field.name).cast(StringType))
        columns.add(col(field.name).cast(StringType))
      })
      if (BusinessEntity.Contact.name().eq(entity))
        // if entity is contact, account id is keyword
        table.withColumn(role.name(), compressUdf(map(columns.toSeq: _*)))
          .select(docIdCol, accountId, role.name())
          .saveToEs(indexName, baseConfig + ("es.mapping.id" -> docIdCol))
      else
        table.withColumn(role.name(), compressUdf(map(columns.toSeq: _*)))
          .select(docIdCol, role.name())
          .saveToEs(indexName, baseConfig + ("es.mapping.id" -> docIdCol))
    } else if (role == TableRoleInCollection.AccountLookup) {
      // group by account Id, parse the atlas lookup key
      val nameUdf = udf((atlasKey: String) => atlasKey.substring(0, atlasKey.lastIndexOf("_")))
      val valueUdf = udf((atlasKey: String) => atlasKey.substring(atlasKey.lastIndexOf("_") + 1));
      val generated: DataFrame = table.withColumn("lookupColumnName", nameUdf(col(InterfaceName.AtlasLookupKey.name())))
        .withColumn("lookupColumnVal", valueUdf(col(InterfaceName.AtlasLookupKey.name())))
        .filter(col("lookupColumnName") =!= InterfaceName.AccountId.name)
        .groupBy(InterfaceName.AccountId.name())
        .pivot("lookupColumnName")
        .agg(first("lookupColumnVal"))
        .drop("lookupColumnName", "lookupColumnVal")
      val columns: mutable.LinkedHashSet[Column] = mutable.LinkedHashSet[Column]()
      generated.schema.fields.foreach((field: StructField) => {
        columns.add(lit(field.name).cast(StringType))
        columns.add(col(field.name).cast(StringType))
      })
      generated.withColumn(role.name(), map(columns.toSeq: _*)).select(docIdCol, role.name())
        .saveToEs(indexName, baseConfig + ("es.mapping.id" -> docIdCol))
    }

  }

}
