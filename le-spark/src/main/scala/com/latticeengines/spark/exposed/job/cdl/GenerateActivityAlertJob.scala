package com.latticeengines.spark.exposed.job.cdl

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant}

import com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants
import com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants.Alert
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream.StreamType.{DnbIntentData, MarketingActivity, WebVisit}
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants
import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.metadata.InterfaceName.{AlertData, AlertName, CreationTimestamp}
import com.latticeengines.domain.exposed.spark.cdl.ActivityAlertJobConfig
import com.latticeengines.domain.exposed.util.TimeLineStoreUtils.TimelineStandardColumn
import com.latticeengines.spark.aggregation.ConcatStringsUDAF
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
  private val title = InterfaceName.Title.name
  private val eventTime = TimelineStandardColumn.EventDate.getColumnName
  private val streamType = TimelineStandardColumn.StreamType.getColumnName
  private val detail1 = TimelineStandardColumn.Detail1.getColumnName
  private val detail2 = TimelineStandardColumn.Detail2.getColumnName
  private val pageVisit = Alert.COL_PAGE_VISITS
  private val pageName = Alert.COL_PAGE_NAME
  private val activeContacts = Alert.COL_ACTIVE_CONTACTS
  private val maCounts = Alert.COL_TOTAL_MA_COUNTS
  private val accountStage = Alert.COL_STAGE
  private val internalRankCol = "__RANK"
  private val internalPrevCol = "__PREV"
  private val internalCurrCol = "__CURR"
  private val maxScore = "MAX_SCORE"
  private val buyingStage = "Buying Stage"
  private val researchingStage = "Researching Stage"
  private val titles = Alert.COL_TITLES
  private val titleCnt = Alert.COL_TITLE_CNT
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
        case Alert.ANONYMOUS_WEB_VISITS =>
          Some(generateAnonymousWebVisitsAlerts(qualifiedTimelineDf, startTimestamp, endTimestamp))
        case Alert.RE_ENGAGED_ACTIVITY =>
          Some(generateReEngagedActivity(timelineDf, startTimestamp, endTimestamp))
        case Alert.HIGH_ENGAGEMENT_IN_ACCOUNT =>
          Some(generateHighEngagementInAccountAlerts(qualifiedTimelineDf, startTimestamp, endTimestamp))
        case Alert.ACTIVE_CONTACT_WEB_VISITS =>
          Some(generateActiveContactWebVisitsAlerts(qualifiedTimelineDf, startTimestamp, endTimestamp))
        case Alert.BUYING_INTENT_AROUND_PRODUCT_PAGES =>
          Some(generateIntentAroundProductAlerts(qualifiedTimelineDf, buyingStage, startTimestamp, endTimestamp))
        case Alert.RESEARCHING_INTENT_AROUND_PRODUCT_PAGES =>
          Some(generateIntentAroundProductAlerts(qualifiedTimelineDf, researchingStage, startTimestamp, endTimestamp))
        case _ => None
      }

      newAlertDfOpt
        .map(_.withColumn(AlertName.name, lit(alertName)))
        .map(mergedAlertDf.unionByName)
        .getOrElse(mergedAlertDf)
    }
    val dedupedAlertDf = if (!config.dedupAlert) {
      alertDf
    } else {
      filterDuplicateAlerts(alertDf, alertIdx.map(lattice.input(_)))
    }

    val mergedAlertDf = alertIdx.map(idx => lattice.input(idx).unionByName(dedupedAlertDf)).getOrElse(dedupedAlertDf)
    lattice.output = mergedAlertDf :: dedupedAlertDf :: Nil
    // count and output whether there are new alert or not
    // TODO change to json object if needed
    lattice.outputStr = dedupedAlertDf.count.toString
  }

  def filterDuplicateAlerts(newAlertDf: DataFrame, existingAlertDf: Option[DataFrame]): DataFrame = {
    val joinCols = Seq(accountId, CreationTimestamp.name, AlertName.name)
    existingAlertDf
      .map(_.select(joinCols.map(col): _*))
      .map(df => newAlertDf.join(df, joinCols, "leftanti"))
      .getOrElse(newAlertDf)
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

  def generateAnonymousWebVisitsAlerts(timelineDf: DataFrame, startTime: Long, endTime: Long): DataFrame = {
    val contactCntDf = getActiveContactsInTimeRange(timelineDf)

    val totalPageVisitDf = timelineDf
      .filter(col(streamType).equalTo(WebVisit.name))
      .filter(col(detail2).isNotNull)
      .withColumn(pageName, explode(split(col(detail2), ",")))
      .groupBy(accountId)
      .agg(count("*").as(pageVisit))

    // ones without any active contact -- anonymous visit
    addTimeRange(totalPageVisitDf.join(contactCntDf, Seq(accountId), "leftanti"), startTime, endTime)
      .select(col(accountId), col(CreationTimestamp.name), packAlertData(pageVisit))
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
      .agg(count("*").as(Alert.COL_RE_ENGAGED_CONTACTS)) //

    // TODO maybe group by account and only issue one alert
    addTimeRange(reEngagedDf, startTime, endTime)
      .select(col(accountId), col(CreationTimestamp.name), packAlertData(Alert.COL_RE_ENGAGED_CONTACTS))
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

  def generateHighEngagementInAccountAlerts(timelineDf: DataFrame, startTime: Long, endTime: Long): DataFrame = {
    val maCountsDf = getMarketingActivityCountsInTimeRange(timelineDf)

    // have more than 5 marketing activity
    addTimeRange(timelineDf
      .join(maCountsDf, Seq(accountId))
      .filter(maCountsDf.col(maCounts).gt(5)), startTime, endTime)
      .select(col(accountId), col(CreationTimestamp.name), packAlertData(maCounts))
      .distinct()
  }

  def generateActiveContactWebVisitsAlerts(timelineDf: DataFrame, startTime: Long, endTime: Long): DataFrame = {
    val contactCntTitleDf = getActiveContactsTitlesInTimeRange(timelineDf)

    val maCntDf = getMarketingActivityCountsInTimeRange(timelineDf)

    // a) no. of known contacts > 0 and no. marketing activity > 0
    // b) Show the titles of those known contacts
    addTimeRange(maCntDf
      .filter(col(maCounts).gt(0))
      .join(contactCntTitleDf, Seq(accountId))
      .filter(contactCntTitleDf.col(activeContacts).gt(0)), startTime, endTime)
      .select(col(accountId), col(CreationTimestamp.name), packAlertData(activeContacts, titles, titleCnt))
  }

  def generateIntentAroundProductAlerts(timelineDf: DataFrame, stage: String, startTime: Long, endTime: Long): DataFrame = {
    val topPageVisitDf = getPageVisitCountInTimeRange(timelineDf)

    val maxScoreDf = timelineDf
      .filter(timelineDf.col(streamType).equalTo(DnbIntentData.name))
      .groupBy(col(accountId))
      .agg(max(col(detail2).cast(DoubleType)).as(maxScore))

    val intentDf = if (stage.equalsIgnoreCase(buyingStage))
      maxScoreDf
        .filter(col(maxScore).geq(buyingStageThreshold))
        .withColumn(accountStage, lit(buyingStage))
    else
      maxScoreDf
        .filter(col(maxScore).lt(buyingStageThreshold))
        .withColumn(accountStage, lit(researchingStage))

    // a) highest no. web visit data of product page group > 0
    // b) account intent is buying or researching stage
    addTimeRange(topPageVisitDf
      .filter(col(pageVisit).gt(0))
      .join(intentDf, Seq(accountId)), startTime, endTime)
      .select(col(accountId), col(CreationTimestamp.name), packAlertData(pageName, accountStage))
  }

  def getMarketingActivityCountsInTimeRange(timelineDf: DataFrame): DataFrame = {
    timelineDf
      .filter(col(streamType).equalTo(MarketingActivity.name))
      .groupBy(accountId)
      .agg(count(streamType).as(maCounts))
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

  def getActiveContactsTitlesInTimeRange(timelineDf: DataFrame): DataFrame = {
    // Define the UDAF to concat titles
    val concatTitles = new ConcatStringsUDAF(title, ",")

    timelineDf
      .filter(col(accountId).isNotNull.and(col(accountId).notEqual(anonymousId)))
      .filter(col(contactId).isNotNull.and(col(contactId).notEqual(anonymousId)))
      .select(col(accountId), col(contactId), col(title))
      .distinct()
      .groupBy(accountId)
      .agg(countDistinct(contactId).as(activeContacts), concatTitles(col(title)).as(titles), count(col(title)).as(titleCnt))
  }

  def filterByTimeRange(timelineDf: DataFrame, startTime: Long, endTime: Long): DataFrame = {
    timelineDf.filter(col(eventTime).geq(startTime).and(col(eventTime).leq(endTime)))
  }

  def addTimeRange(df: DataFrame, startTime: Long, endTime: Long): DataFrame = {
    df.withColumn(Alert.COL_START_TIMESTAMP, lit(startTime))
      .withColumn(Alert.COL_END_TIMESTAMP, lit(endTime))
      // NOTE currently endTime is always current timestamp
      .withColumn(CreationTimestamp.name, lit(endTime))
  }

  def packAlertData(cols: String*): Column = {
    to_json(struct(
      struct(cols.head, cols.tail: _*).as(Alert.COL_ALERT_DATA),
      col(Alert.COL_START_TIMESTAMP),
      col(Alert.COL_END_TIMESTAMP))).as(AlertData.name)
  }
}
