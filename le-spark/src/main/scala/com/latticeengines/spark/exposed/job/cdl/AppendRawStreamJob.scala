package com.latticeengines.spark.exposed.job.cdl

import java.net.URLDecoder
import java.time.Instant
import java.time.temporal.ChronoUnit

import com.google.common.base.CaseFormat
import com.latticeengines.common.exposed.util.DateTimeUtils.{dateToDayPeriod, toDateOnlyFromMillis}
import com.latticeengines.domain.exposed.cdl.activity.{ActivityStoreConstants, EmptyStreamException}
import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.metadata.InterfaceName._
import com.latticeengines.domain.exposed.spark.cdl.AppendRawStreamConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.{DeriveAttrsUtils, MergeUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
 * Process activity stream imports and merge with current batch store if any
 */
class AppendRawStreamJob extends AbstractSparkJob[AppendRawStreamConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[AppendRawStreamConfig]): Unit = {
    // prepare input/vars
    val config: AppendRawStreamConfig = lattice.config
    val hasImport = config.matchedRawStreamInputIdx != null
    val hasMaster = config.masterInputIdx != null
    val discardAttrs = config.discardAttrs.asScala
    // partition by dateId
    setPartitionTargets(0, Seq(StreamDateId.name()), lattice)

    // calculation
    var df: DataFrame = if (hasImport) {
      val getDate = udf {
        time: Long => toDateOnlyFromMillis(time.toString)
      }
      val getDateId = udf {
        time: Long => dateToDayPeriod(toDateOnlyFromMillis(time.toString))
      }

      // add date & dateId
      var mdf: DataFrame = lattice.input(config.matchedRawStreamInputIdx)
      mdf = parseUtmCodes(mdf
        .withColumn(__StreamDate.name, getDate(mdf.col(config.dateAttr)))
        .withColumn(StreamDateId.name, getDateId(mdf.col(config.dateAttr)))
        .withColumn(LastActivityDate.name, mdf.col(config.dateAttr)))
      if (hasMaster) {
        mdf = MergeUtils.concat2(mdf, lattice.input(config.masterInputIdx))
      }
      mdf
    } else {
      lattice.input(config.masterInputIdx)
    }
    if (config.retentionDays != null) {
      // apply retention policy and remove old data (keep the entire day for now)
      df = df.filter(df.col(StreamDateId.name).geq(getStartDateId(config.retentionDays, config.currentEpochMilli)))
    }

    if (Option(config.reducer).isDefined) {
      val reducer = Option(config.reducer).get
      if (DeriveAttrsUtils.isTimeReducingOperation(reducer.getOperator)) {
        reducer.getGroupByFields.append(__StreamDate.name) // separate by each day if filter is applied for time
      }
      df = DeriveAttrsUtils.applyReducer(df, reducer)
    }

    df = discardAttrs.foldLeft(df)((accDf, attr) => {
      if (accDf.columns.contains(attr)) {
        accDf.drop(attr)
      } else {
        accDf
      }
    })

    val cnt = df.count
    if (cnt == 0) {
      throw new EmptyStreamException(StringUtils.defaultIfEmpty(config.streamName, ""), config.retentionDays, config.currentEpochMilli)
    }

    lattice.output = df :: Nil
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

  private def getStartDateId(retentionDays: Int, epochMilli: Long): Int = {
    val startTime = Instant.ofEpochMilli(epochMilli).minus(retentionDays, ChronoUnit.DAYS).toEpochMilli.toString
    dateToDayPeriod(toDateOnlyFromMillis(startTime))
  }
}
