package com.latticeengines.spark.exposed.job.cdl

import java.net.URLDecoder
import java.util.regex.Pattern

import com.google.common.base.CaseFormat
import com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants
import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.metadata.InterfaceName._
import com.latticeengines.domain.exposed.spark.cdl.EnrichWebVisitJobConfig
import com.latticeengines.domain.exposed.util.ActivityStoreUtils
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.MergeUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions.{callUDF, col, lit, udf}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters.mapAsScalaMapConverter

class EnrichWebVisitJob extends AbstractSparkJob[EnrichWebVisitJobConfig] {

  val partitionKey: String = StreamDateId.name()

  override def runJob(spark: SparkSession, lattice: LatticeContext[EnrichWebVisitJobConfig]): Unit = {
    val config: EnrichWebVisitJobConfig = lattice.config
    val selectedAttributes = config.selectedAttributes.asScala

    var matchedTable: DataFrame = null
    if (config.matchedWebVisitInputIdx != null) {
      matchedTable = lattice.input(config.matchedWebVisitInputIdx)
      if (config.latticeAccountTableIdx != null) {
        val latticeAccountTable: DataFrame = lattice.input(config.latticeAccountTableIdx)
        matchedTable = matchedTable.join(latticeAccountTable, Seq(AccountId.name), "left")
      }

      matchedTable = selectedAttributes.foldLeft(matchedTable) {
        case (df, (columnName, displayName)) => addAllNullsIfMissingAndRename(df, columnName, displayName)
      }
      matchedTable = matchedTable.select(selectedAttributes.values.toList.map(columnName =>
        matchedTable.col(columnName)): _*)
    }
    if (config.masterInputIdx != null) {
      val masterTable: DataFrame = lattice.input(config.masterInputIdx)
      matchedTable = merge(masterTable, matchedTable)
    }
    if (config.catalogInputIdx != null) {
      val catalogTable: DataFrame = lattice.input(config.catalogInputIdx)
      val pathPatternMap = catalogTable.select(PathPattern.name(), PathPatternName.name).rdd.map(row => ActivityStoreUtils.modifyPattern(row.getAs
      (PathPattern.name()).toString).r.pattern -> row.getAs(PathPatternName.name()).toString).collectAsMap()
      matchedTable.drop("page_groups")
      matchedTable = populateProductPatternNames(matchedTable, pathPatternMap, "page_url", "page_groups", lattice)
    }
    matchedTable = parseUtmCodes(matchedTable)
    lattice.output = matchedTable :: Nil
  }

  // hard-code parsing utm code (from google analytics) on web visit url for now
  // since we only have one url column and one vendor
  private def parseUtmCodes(df: DataFrame): DataFrame = {
    val urlCol = WebVisitPageUrl.name
    if (!df.columns.contains(urlCol)) {
      df
    } else {
      ActivityStoreConstants.WebVisit.UTM_COLUMNS
        .foldLeft(df)((parsedDf, utmCol) => parseQueryParameter(parsedDf, utmCol, urlCol))
    }
  }

  private def parseQueryParameter(df: DataFrame, col: InterfaceName, urlCol: String): DataFrame = {
    val urlDecode = udf {
      s: String =>
        if (StringUtils.isNotBlank(s)) {
          try {
            Some(URLDecoder.decode(s, "UTF-8"))
          } catch {
            case _: Throwable => None
          }
        } else None
    }
    val queryParam = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, col.name)
    df.withColumn(col.name, urlDecode(callUDF("parse_url", df.col(urlCol), lit("QUERY"), lit(queryParam))))
  }

  def addAllNullsIfMissingAndRename(df: DataFrame, requiredCol: String, displayName: String): DataFrame = {
    val dfColumnNames = df.columns
    val dfColumnNameMaps = dfColumnNames.map(columnName => (columnName.toLowerCase, columnName)).toMap
    if (!dfColumnNameMaps.contains(requiredCol.toLowerCase) && !dfColumnNameMaps.contains(displayName.toLowerCase)) {
      return df.withColumn(displayName, lit(null).cast(StringType))
    } else if (!dfColumnNameMaps.contains(displayName.toLowerCase)) {
      return df.withColumnRenamed(requiredCol, displayName)
    }
    df
  }

  // This is a method to populate product path names for web visit activity data
  def populateProductPatternNames(df: DataFrame, pathPatternMap: scala.collection.Map[Pattern, String],
                                  columnName: String,
                                  displayName: String,
  lattice: LatticeContext[EnrichWebVisitJobConfig])
  : DataFrame = {
    val filterFn = udf((url: String)
    => {
      pathPatternMap.filter(_._1.matcher(url).matches).values.mkString("||")
    })
    logSpark("----- BEGIN SCRIPT OUTPUT -----")
    df.printSchema
    logSpark("----- END SCRIPT OUTPUT -----")
    df.withColumn(displayName, filterFn(col(columnName)))
  }

  private def merge(df: DataFrame, origin: DataFrame): DataFrame = {
    if (origin == null) {
      df
    } else {
      MergeUtils.concat2(origin, df)
    }
  }
}
