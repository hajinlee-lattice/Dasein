package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId
import com.latticeengines.domain.exposed.spark.cdl.PublishVIDataJobConfiguration
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.MergeUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql._

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConverters.mapAsScalaMapConverter

class PublishVIDataJob extends AbstractSparkJob[PublishVIDataJobConfiguration] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[PublishVIDataJobConfiguration]): Unit = {
    val config : PublishVIDataJobConfiguration = lattice.config
    val latticeAccount: DataFrame = lattice.input(config.latticeAccountTableIdx)
    val inputIdx = config.inputIdx.asScala
    val selectedAttributes = config.selectedAttributes.toList
    val webVisitAttributes = config.webVisitAttributes
    val latticeAccountAttributes = config.latticeAccountAttributes.toList
    val esConfigs = config.esConfigs.asScala
    val index = esConfigs.get("esIndex").toString
    val baseConfig : Map[String, String] = Map(
      "es.write.operation" -> "upsert",
      "es.nodes.wan.only" -> "true",
      "es.index.auto.create" -> "false",
      "es.batch.write.refresh" -> "false",
      "es.nodes" -> String.valueOf(esConfigs.get("esHost")),
      "es.port" -> String.valueOf(esConfigs.get("esPorts")),
      "es.net.http.auth.user" -> String.valueOf(esConfigs.get("user")),
      "es.net.http.auth.pass" -> String.valueOf(esConfigs.get("pwd")),
      "es.net.ssl" -> "true")

    var webVisitDf : DataFrame = null
    inputIdx.foreach {
      case (tableName, idx) =>
        val origin: DataFrame = lattice.input(idx)
        val joinedDf = origin.join(latticeAccount.select(latticeAccountAttributes map col: _*), Seq(AccountId.name),
          "left").select(selectedAttributes map col: _*)
        webVisitDf = merge(joinedDf, webVisitDf)
    }
    if (webVisitDf != null) {
      webVisitDf.saveToEs(index, baseConfig)
    }
  }

  private def merge(df: DataFrame, accDf: DataFrame): DataFrame = {
    if (accDf == null) {
      df
    } else {
      MergeUtils.concat2(accDf, df)
    }
  }
}
