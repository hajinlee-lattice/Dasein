package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.common.exposed.util.JsonUtils
import com.latticeengines.domain.exposed.camille.CustomerSpace
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection._
import com.latticeengines.domain.exposed.query.BusinessEntity
import com.latticeengines.domain.exposed.spark.cdl.PublishTableToElasticSearchJobConfiguration
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.ElasticSearchUtils
import com.latticeengines.spark.util.ElasticSearchUtils._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.xerial.snappy.Snappy

import scala.collection.JavaConverters._
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
        else
          saveToESWithMeta(table, indexName, role, docIdCol, baseConfig, true)
      }
    }
  }

  def saveToESWithMeta(table : DataFrame, indexName : String, role : TableRoleInCollection, docIdCol : String,
                     baseConfig : Map[String, String], compressed : Boolean) : Unit = {
    val cols = table.columns
    implicit val encoder = org.apache.spark.sql.Encoders.kryo[(String, scala.collection.immutable.Map[String, Map[String, String]])]
    implicit val encoder2 = org.apache.spark.sql.Encoders.kryo[(String, Array[Byte])]

    if (compressed)
      table.map(row  => (row.getAs[String](docIdCol),
        Snappy.compress(JsonUtils.serialize(Map(role.toString -> row.getValuesMap[String](cols))))
        )).rdd.saveToEsWithMeta(indexName, baseConfig)
    else
      table.map(row  => (row.getAs[String](docIdCol), Map(role.toString -> row.getValuesMap[String](cols))))
        .rdd.saveToEsWithMeta(indexName, baseConfig)

  }

}
