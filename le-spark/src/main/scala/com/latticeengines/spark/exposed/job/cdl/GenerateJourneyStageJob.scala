package com.latticeengines.spark.exposed.job.cdl

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant}

import com.latticeengines.domain.exposed.cdl.activity.{AtlasStream, JourneyStage}
import com.latticeengines.domain.exposed.metadata.InterfaceName.{AccountId, PartitionKey, SortKey, StageName}
import com.latticeengines.domain.exposed.query.AggregateLookup
import com.latticeengines.domain.exposed.spark.cdl.JourneyStageJobConfig
import com.latticeengines.domain.exposed.spark.common.GroupingUtilConfig
import com.latticeengines.domain.exposed.util.TimeLineStoreUtils
import com.latticeengines.domain.exposed.util.TimeLineStoreUtils.TimelineStandardColumn._
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.{GroupingUtil, MergeUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._

class GenerateJourneyStageJob extends AbstractSparkJob[JourneyStageJobConfig] {
  private val INTERNAL_PRIORITY_KEY = "__Priority__"
  private val INTERNAL_STAGENAME_KEY = "__StageName__"
  private val INTERNAL_TIME_KEY = "__Time__"
  private val JOURNEY_STAGE_EVENT_TYPE = "Journey Stage Change"
  private val JOURNEY_STAGE_SOURCE = "Atlas"
  private val JOURNEY_STAGE_RECORD_ID = "js_change"
  private val CHANGE_DETECTION_LOOKBACK_DAYS = 90

  override def runJob(spark: SparkSession, lattice: LatticeContext[JourneyStageJobConfig]): Unit = {
    val config = lattice.config
    val stages = config.journeyStages.asScala
    val defaultStageName = config.defaultStage.getStageName
    val priorityStageMap = stages.map(stage => (stage.getPriority, stage.getStageName)).toMap

    val timelineMaster = lattice.input(config.masterAccountTimeLineIdx)
    val timelineDiff = lattice.input(config.diffAccountTimeLineIdx)

    val eventTimeCol = EventDate.getColumnName
    val priorityToStage = udf((priority: Int) => priorityStageMap(priority))
    val schema = StructType(
      StructField(AccountId.name, StringType, nullable = false) ::
        StructField(eventTimeCol, LongType, nullable = false) ::
        StructField(StageName.name, StringType, nullable = false) :: Nil)
    val initialStageChangeDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    val initialMergedStageDf = if (config.masterJourneyStageIdx != null) {
      lattice.input(config.masterJourneyStageIdx)
    } else {
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    }

    // evaluate journey stages in [startTime, endTime]
    val startTime = config.earliestBackfillEpochMilli.longValue()
    val endTime = config.currentEpochMilli.longValue()
    val step = Duration.ofDays(config.backfillStepInDays).toMillis
    val (masterStageDf, stageChangeDf) = startTime.to(endTime, step)
      .foldLeft((initialMergedStageDf, initialStageChangeDf))((stageTuple, timestamp) => {
        val currTime = Instant.ofEpochMilli(timestamp)
        val (mergedStage, changeDf) = stageTuple

        val mergedPriorityDf = stages.map({ stage =>
          getQualifiedAccounts(timelineMaster, stage, currTime)
            .select(AccountId.name)
            .withColumn(INTERNAL_PRIORITY_KEY, lit(stage.getPriority))
        }).reduce(_ unionByName _)
          .groupBy(AccountId.name)
          .agg(max(INTERNAL_PRIORITY_KEY).as(INTERNAL_PRIORITY_KEY))
        val newMergedStageDf = mergedPriorityDf
          .join(timelineMaster.select(AccountId.name).distinct, Seq(AccountId.name), "right")
          .withColumn(eventTimeCol, lit(currTime.toEpochMilli))
          .withColumn(StageName.name, coalesce(
            priorityToStage(mergedPriorityDf.col(INTERNAL_PRIORITY_KEY)),
            lit(defaultStageName))) // fill default stage name for all account with event
          .drop(INTERNAL_PRIORITY_KEY)

        // calculate changed journey stages and merged master journey stages
        val newChangeDf = mergedStage
          // re-generate stage events for account that does not change stage but last event is published long time ago
          .filter(mergedStage.col(eventTimeCol)
            .geq(currTime.minus(CHANGE_DETECTION_LOOKBACK_DAYS, ChronoUnit.DAYS).toEpochMilli))
          .join(newMergedStageDf, Seq(AccountId.name), "outer")
          .filter(!(mergedStage.col(StageName.name) <=> newMergedStageDf.col(StageName.name)))
          // when existing stage is default and there's no new stage generated, journey stage is NOT changed
          .filter(!(mergedStage.col(StageName.name).equalTo(defaultStageName) && newMergedStageDf.col(StageName.name).isNull))
          .withColumn(INTERNAL_STAGENAME_KEY, coalesce( //
            newMergedStageDf.col(StageName.name), //
            lit(defaultStageName) // previously have stage but currently not, set to default stage
          ))
          .withColumn(INTERNAL_TIME_KEY, lit(currTime.toEpochMilli))
          .select(AccountId.name, INTERNAL_STAGENAME_KEY, INTERNAL_TIME_KEY)
          .withColumnRenamed(INTERNAL_STAGENAME_KEY, StageName.name)
          .withColumnRenamed(INTERNAL_TIME_KEY, eventTimeCol)

        val newAccStageDf = MergeUtils.merge2(mergedStage, newChangeDf, Seq(AccountId.name), Set(), overwriteByNull = false)
        (newAccStageDf, newChangeDf.unionByName(changeDf))
      })

    // merge changed stage
    val stageChangeEventDf = toTimelineEvent(stageChangeDf, config.accountTimeLineVersion, config.accountTimeLineId)
    val mergedMasterTimeLine = MergeUtils.concat2(timelineMaster, stageChangeEventDf)
    val mergedDiffTimeLine = MergeUtils.concat2(timelineDiff, stageChangeEventDf)

    // [ master timeline, diff timeline, master journey stage ]
    lattice.output = mergedMasterTimeLine :: mergedDiffTimeLine :: masterStageDf :: Nil
  }

  def toTimelineEvent(stageDf: DataFrame, timelineVersion: String, timelineId: String): DataFrame = {
    val generateSortKey = udf {
      (eventTimeStamp: Long, recordId: String) => TimeLineStoreUtils.generateSortKey(eventTimeStamp, recordId)
    }

    val generatePartitionKey = udf {
      accountId: String => TimeLineStoreUtils.generatePartitionKey(timelineVersion, timelineId, accountId)
    }

    val recordIdCol = RecordId.getColumnName
    stageDf
      // can use deterministic record ID because if we have two stage evaluated at the same time,
      // new one should overwrite old one anyways
      .withColumn(recordIdCol, lit(JOURNEY_STAGE_RECORD_ID))
      .withColumn(PartitionKey.name, generatePartitionKey(col(AccountId.name)))
      .withColumn(SortKey.name, generateSortKey(col(EventDate.getColumnName), col(recordIdCol)))
      .withColumn(EventType.getColumnName, lit(JOURNEY_STAGE_EVENT_TYPE))
      .withColumnRenamed(StageName.name, Detail1.getColumnName)
      .withColumn(TrackedBySystem.getColumnName, lit(JOURNEY_STAGE_SOURCE))
      .withColumn(StreamType.getColumnName, lit(AtlasStream.StreamType.JourneyStage.name))
  }

  def getQualifiedAccounts(df: DataFrame, stage: JourneyStage, currTime: Instant): DataFrame = {
    stage.getPredicates.asScala.map(sp => {
      val gdf = GroupingUtil.getGroupedDf(df, GroupingUtilConfig.from(stage, sp, currTime))
      gdf.where(gdf.col(AggregateLookup.Aggregator.COUNT.name).geq(lit(sp.getNoOfEvents))).select(AccountId.name)
    }).reduce(_ union _)
  }
}
