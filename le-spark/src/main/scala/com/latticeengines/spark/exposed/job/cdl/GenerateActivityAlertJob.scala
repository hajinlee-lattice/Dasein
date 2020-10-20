package com.latticeengines.spark.exposed.job.cdl

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant}

import com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants
import com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants.Alert
import com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants.Alert.{COL_ALERT_DATA, COL_END_TIMESTAMP, COL_RE_ENGAGED_CONTACTS, COL_START_TIMESTAMP}
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream.StreamType.{DnbIntentData, MarketingActivity, WebVisit}
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants
import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.metadata.InterfaceName.{AlertData, AlertName, CreationTimestamp}
import com.latticeengines.domain.exposed.spark.cdl.ActivityAlertJobConfig
import com.latticeengines.domain.exposed.util.TimeLineStoreUtils.TimelineStandardColumn
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._

/**
 * spark job to generate predefined alert based on timeline data.
 */
class GenerateActivityAlertJob extends AbstractSparkJob[ActivityAlertJobConfig] {

  private val accountId = InterfaceName.AccountId.name
  private val contactId = InterfaceName.ContactId.name
  private val eventTime = TimelineStandardColumn.EventDate.getColumnName
  private val streamType = TimelineStandardColumn.StreamType.getColumnName
  private val detail1 = TimelineStandardColumn.Detail1.getColumnName
  private val detail2 = TimelineStandardColumn.Detail2.getColumnName
  private val pageVisit = ActivityStoreConstants.Alert.COL_PAGE_VISITS
  private val pageName = ActivityStoreConstants.Alert.COL_PAGE_NAME
  private val pageVisitTime = ActivityStoreConstants.Alert.COL_PAGE_VISIT_TIME
  private val prevPageVisitTime = ActivityStoreConstants.Alert.COL_PREV_PAGE_VISIT_TIME
  private val activeContacts = ActivityStoreConstants.Alert.COL_ACTIVE_CONTACTS
  private val internalRankCol = "__RANK"
  private val internalPrevCol = "__PREV"
  private val internalCurrCol = "__CURR"
  private val buyingStageThreshold = ActivityStoreConstants.DnbIntent.BUYING_STAGE_THRESHOLD
  private val anonymousId = DataCloudConstants.ENTITY_ANONYMOUS_ID

  override def runJob(spark: SparkSession, lattice: LatticeContext[ActivityAlertJobConfig]): Unit = {
    val config = lattice.config
    val alertIdx = Option(config.masterAlertIdx)
    val timelineDf = lattice.input(config.masterAccountTimeLineIdx)
    val qualificationPeriods = config.alertNameToQualificationPeriodDays.asScala

    val schema = StructType(
      StructField(accountId, StringType, nullable = false) ::
        StructField(CreationTimestamp.name, LongType, nullable = false) ::
        StructField(AlertName.name, StringType, nullable = false) ::
        StructField(AlertData.name, StringType, nullable = false) :: Nil)
    val emptyAlertDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    val endTimestamp = config.currentEpochMilli

    val alertDf = qualificationPeriods.foldLeft(emptyAlertDf) { (mergedAlertDf, alertNameDays) =>
      val (alertName, periodInDays) = alertNameDays
      val startTimestamp = Instant
        .ofEpochMilli(endTimestamp)
        .minus(periodInDays, ChronoUnit.DAYS)
        .toEpochMilli

      val qualifiedTimelineDf = filterByTimeRange(timelineDf, startTimestamp, endTimestamp)

      // generate new alerts
      val newAlertDfOpt = alertName match {
        case Alert.INC_WEB_ACTIVITY =>
          Some(generateIncreasedWebActivityAlerts(qualifiedTimelineDf, startTimestamp, endTimestamp))
        case Alert.INC_WEB_ACTIVITY_ON_PRODUCT =>
          Some(generateIncreasedWebActivityOnProductAlerts(qualifiedTimelineDf, startTimestamp, endTimestamp))
        case Alert.RE_ENGAGED_ACTIVITY =>
          Some(generateReEngagedActivity(timelineDf, startTimestamp, endTimestamp))
        // intent need data in all time range
        case Alert.SHOWN_INTENT =>
          Some(generateShownIntentAlerts(qualifiedTimelineDf, startTimestamp, endTimestamp))
        case _ => None
      }

      newAlertDfOpt
        .map(_.withColumn(AlertName.name, lit(alertName)))
        .map(mergedAlertDf.unionByName)
        .getOrElse(mergedAlertDf)
    }

    val mergedAlertDf = alertIdx.map(idx => lattice.input(idx).unionByName(alertDf)).getOrElse(alertDf)
    lattice.output = mergedAlertDf :: alertDf :: Nil
    // count and output whether there are new alert or not
    // TODO change to json object if needed
    lattice.outputStr = alertDf.count.toString
  }

  def generateIncreasedWebActivityAlerts(timelineDf: DataFrame, startTime: Long, endTime: Long): DataFrame = {
    val contactCntDf = getActiveContactsInTimeRange(timelineDf)

    // might need to rely on spark to optimize this since we make lookback period separate for each alert
    val topPageVisitDf = getPageVisitCountInTimeRange(timelineDf)

    // have more than one active contacts
    addTimeRange(topPageVisitDf
      .join(contactCntDf, Seq(accountId))
      .filter(contactCntDf.col(activeContacts).gt(0)), startTime, endTime)
      .select(col(accountId), col(CreationTimestamp.name), packAlertData(activeContacts, pageVisit, pageName))
  }

  def generateIncreasedWebActivityOnProductAlerts(timelineDf: DataFrame, startTime: Long, endTime: Long): DataFrame = {
    val contactCntDf = getActiveContactsInTimeRange(timelineDf)

    val topPageVisitDf = getPageVisitCountInTimeRange(timelineDf)

    // ones without any active contact
    addTimeRange(topPageVisitDf.join(contactCntDf, Seq(accountId), "leftanti"), startTime, endTime)
      .select(col(accountId), col(CreationTimestamp.name), packAlertData(pageVisit, pageName))
  }

  def generateReEngagedActivity(timelineDf: DataFrame, startTime: Long, endTime: Long): DataFrame = {
    val inactivePeriodInMillis = Duration.ofDays(ActivityStoreConstants.Alert.RE_ENGAGED_QUIET_PERIOD_IN_DAYS).toMillis

    val marketingEventDf = timelineDf
      .filter(col(streamType).equalTo(MarketingActivity.name))
      .filter(col(accountId).isNotNull.and(col(accountId).notEqual(anonymousId)))
      .filter(col(contactId).isNotNull.and(col(contactId).notEqual(anonymousId)))

    val window = Window
      .partitionBy(accountId, contactId)
      .orderBy(col(eventTime).desc)
    val prevVisitTime = lead(col(eventTime), 1).over(window)
    val firstEventInRangeWindow = Window
      .partitionBy(accountId, contactId)
      .orderBy(col(internalRankCol).desc)

    val tmpCol = "__TMP_RANK"
    val reEngagedDf = marketingEventDf
      .withColumn(internalRankCol, row_number.over(window))
      .withColumn(internalPrevCol, prevVisitTime)
      .filter(col(eventTime).geq(startTime).and(col(eventTime).leq(endTime)))
      // first visit in time range
      .withColumn(tmpCol, row_number.over(firstEventInRangeWindow))
      .filter(col(tmpCol) === 1)
      // duration between visit to this page and the visit before it exceed threshold
      .filter(col(internalPrevCol).lt(col(eventTime) - inactivePeriodInMillis))
      .groupBy(accountId)
      .agg(count("*").as(COL_RE_ENGAGED_CONTACTS)) //

    // TODO maybe group by account and only issue one alert
    addTimeRange(reEngagedDf, startTime, endTime)
      .select(col(accountId), col(CreationTimestamp.name), packAlertData(COL_RE_ENGAGED_CONTACTS))
  }

  def generateShownIntentAlerts(timelineDf: DataFrame, startTime: Long, endTime: Long): DataFrame = {
    // FIXME remove when we are sure we don't need this
    val accWithVisitDf = timelineDf
      .filter(timelineDf.col(streamType).equalTo(WebVisit.name))
      .groupBy(accountId)
      .agg(count("*").as(pageVisit))

    // latest intent per model
    val window = Window
      .partitionBy(accountId, detail1)
      .orderBy(col(eventTime).desc)

    val countCol = "__CNT"
    // get max buying score per model (detail1)
    val latestIntentDf = timelineDf
      .filter(timelineDf.col(streamType).equalTo(DnbIntentData.name))
      .withColumn(internalRankCol, row_number.over(window))
      .filter(col(internalRankCol).equalTo(1))
      .withColumn(internalCurrCol, col(detail2).cast(DoubleType))
      .filter(col(internalCurrCol).isNotNull)
      .filter(col(detail1).isNotNull)

    // find models in each stage
    val buyingIntentCntDf = latestIntentDf
      .filter(col(internalCurrCol).geq(buyingStageThreshold))
      .groupBy(accountId)
      .agg(count("*").as(countCol))
    val researchingIntentCntDf = latestIntentDf
      .filter(col(internalCurrCol).lt(buyingStageThreshold))
      .groupBy(accountId)
      .agg(count("*").as(countCol))

    val buyIntentCntCol = ActivityStoreConstants.Alert.COL_NUM_BUY_INTENTS
    val researchIntentCntCol = ActivityStoreConstants.Alert.COL_NUM_RESEARCH_INTENTS
    // decide whether to show buying or researching stage in entire alert
    val buyingIntentCntCol = coalesce(buyingIntentCntDf.col(countCol), lit(0).cast(LongType))
    val researchingIntentCntCol = coalesce(researchingIntentCntDf.col(countCol), lit(0).cast(LongType))

    val alertDf = buyingIntentCntDf
      .join(researchingIntentCntDf, Seq(accountId), "outer")
      .withColumn(buyIntentCntCol, buyingIntentCntCol)
      .withColumn(researchIntentCntCol, researchingIntentCntCol)
      .select(accountId, buyIntentCntCol, researchIntentCntCol)

    // find all with product visit
    addTimeRange(alertDf, startTime, endTime)
      .select(col(accountId), col(CreationTimestamp.name), packAlertData(buyIntentCntCol, researchIntentCntCol))
  }

  def getPageVisitCountInTimeRange(timelineDf: DataFrame): DataFrame = {
    // detail2 contains comma separated
    val accPageVisitDf = timelineDf
      .filter(col(streamType).equalTo(WebVisit.name))
      .filter(col(detail2).isNotNull)
      .withColumn(pageName, explode(split(col(detail2), ",")))
      .groupBy(accountId, pageName)
      .agg(count("*").as(pageVisit))

    val window = Window
      .partitionBy(col(accountId))
      .orderBy(col(pageVisit).desc)

    // select the product page receiving max no. visits within account
    val rankedPageVisit = accPageVisitDf.withColumn(internalRankCol, row_number.over(window))
    rankedPageVisit
      .filter(col(internalRankCol) === 1)
      .drop(internalRankCol)
  }

  def getActiveContactsInTimeRange(timelineDf: DataFrame): DataFrame = {
    timelineDf
      .filter(col(accountId).isNotNull.and(col(accountId).notEqual(anonymousId)))
      .filter(col(contactId).isNotNull.and(col(contactId).notEqual(anonymousId)))
      .groupBy(accountId)
      .agg(countDistinct(contactId).as(activeContacts))
  }

  def filterByTimeRange(timelineDf: DataFrame, startTime: Long, endTime: Long): DataFrame = {
    timelineDf.filter(col(eventTime).geq(startTime).and(col(eventTime).leq(endTime)))
  }

  def addTimeRange(df: DataFrame, startTime: Long, endTime: Long): DataFrame = {
    df.withColumn(COL_START_TIMESTAMP, lit(startTime))
      .withColumn(COL_END_TIMESTAMP, lit(endTime))
      // NOTE currently endTime is always current timestamp
      .withColumn(CreationTimestamp.name, lit(endTime))
  }

  def packAlertData(cols: String*): Column = {
    to_json(struct(
      struct(cols.head, cols.tail: _*).as(COL_ALERT_DATA),
      col(COL_START_TIMESTAMP),
      col(COL_END_TIMESTAMP))).as(AlertData.name)
  }
}
