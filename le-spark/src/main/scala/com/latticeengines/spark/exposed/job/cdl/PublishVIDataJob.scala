package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.InterfaceName._
import com.latticeengines.domain.exposed.spark.cdl.PublishVIDataJobConfiguration
import com.latticeengines.domain.exposed.util.ActivityStoreUtils
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.MergeUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql._
import org.json4s.jackson.Serialization

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.immutable

class PublishVIDataJob extends AbstractSparkJob[PublishVIDataJobConfiguration] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[PublishVIDataJobConfiguration]): Unit = {
    val config : PublishVIDataJobConfiguration = lattice.config
    val latticeAccount: DataFrame = lattice.input(config.latticeAccountTableIdx)
    val inputIdx = config.inputIdx
    val selectedAttributes = config.selectedAttributes
    val esConfigs = config.esConfigs.asScala
    val filterParams = config.filterParams.asScala
    val index = esConfigs.get("esIndex").toString
    val webVisitTableNameIsMaps = config.webVisitTableNameIsMaps.asScala
    val isTest = config.isTest

    var webVisitDf : DataFrame = null
    webVisitTableNameIsMaps.foreach {
      case (streamId, tableName) =>
        val idx : Int = inputIdx.get(tableName)
        val origin: DataFrame = lattice.input(idx)
        val addUrlDf: DataFrame = populateProductPatternNames(origin, streamId, lattice)
        val joinedDf: DataFrame = addUrlDf.join(latticeAccount, Seq(AccountId.name), "left")
        val formatedDf: DataFrame = selectedAttributes.foldLeft(joinedDf) {
          (df, columnName) => addAllNullsIfMissing(df, columnName)
        }
        webVisitDf = merge(formatedDf, webVisitDf)
    }
    webVisitDf = webVisitDf.select(selectedAttributes.map(columnName =>
      webVisitDf.col(columnName)):_*)
    if (webVisitDf != null && !isTest) {
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
      webVisitDf.saveToEs(index, baseConfig)
    }

    val filterMap = immutable.Map(filterParams.map {
      case (placeHolderName, columnName) =>


        if (columnName.equals("UrlCategories")) {//deal the column contains Seq(String) case.
          val explodeColumn = "UrlCategory"
          val filterDf = webVisitDf.withColumn(explodeColumn, explode(col("UrlCategories")))
          val filterValueList = filterDf.select(explodeColumn).filter(col(explodeColumn).isNotNull).groupBy(explodeColumn)
            .agg(count("*").alias("num")).sort(desc("num")).limit(3).select(explodeColumn).collect().toList
          (placeHolderName, filterValueList.map(row => row.mkString))
        } else {
          val filterValueList = webVisitDf.select(columnName).filter(col(columnName).isNotNull).groupBy(columnName)
            .agg(count("*").alias("num")).sort(desc("num")).limit(30).select(columnName).collect().toList
          (placeHolderName, filterValueList.map(row => row.mkString))
        }
    }.toSeq:_*)
    if (isTest) {
      lattice.output = webVisitDf :: Nil
    }
    lattice.outputStr = Serialization.write(filterMap)(org.json4s.DefaultFormats)
  }

  def addAllNullsIfMissing(df: DataFrame, requiredCol: String): DataFrame = {
    val dfColumnNames = df.columns
    val dfColumnNameMaps = dfColumnNames.map(columnName => (columnName.toLowerCase, columnName)).toMap
    if (!dfColumnNameMaps.contains(requiredCol.toLowerCase)) {
      return df.withColumn(requiredCol, lit(null).cast(StringType))
    }
    df
  }

  private def merge(df: DataFrame, accDf: DataFrame): DataFrame = {
    if (accDf == null) {
      df
    } else {
      MergeUtils.concat2(accDf, df)
    }
  }

  // This is a method to populate product path names for web visit activity data
  def populateProductPatternNames(df: DataFrame, streamId: String, lattice: LatticeContext[PublishVIDataJobConfiguration])
  : DataFrame = {
    if ( streamId == null || streamId.isEmpty) {
      return df
    }
    val metadataMap = lattice.config.dimensionMetadataMap.asScala.mapValues(_.asScala)
    val metadataInStream = metadataMap(streamId)
    if (metadataInStream.isEmpty || !metadataInStream.contains(PathPatternId.name)) {
      return df
    }

    val dimValues = metadataInStream(PathPatternId.name).getDimensionValues.map(_.asScala.toMap).toList
    val pathPatternMap = immutable.Map(dimValues.map(p => (ActivityStoreUtils.modifyPattern(p(PathPattern
      .name()).asInstanceOf[String]).r.pattern, p(PathPatternName.name()).asInstanceOf[String])): _*)
    val filterFn = udf((url: String)
    => {
      pathPatternMap.filter(_._1.matcher(url).matches).values.toSeq
    })
    logSpark("----- BEGIN SCRIPT OUTPUT -----")
    df.printSchema
    logSpark("----- END SCRIPT OUTPUT -----")
    df.withColumn("UrlCategories", filterFn(col(WebVisitPageUrl.name)))
  }
}
