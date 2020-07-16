package com.latticeengines.spark.exposed.job.cdl

import java.time.Instant
import java.time.temporal.ChronoUnit

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
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

class GenerateJourneyStageJob extends AbstractSparkJob[JourneyStageJobConfig] {
  private val INTERNAL_PRIORITY_KEY = "__Priority__"
  private val INTERNAL_STAGENAME_KEY = "__StageName__"
  private val INTERNAL_TIME_KEY = "__Time__"
  private val JOURNEY_STAGE_EVENT_TYPE = "Journey Stage Change"
  private val JOURNEY_STAGE_SOURCE = "Atlas"
  private val CHANGE_DETECTION_LOOKBACK_DAYS = 30

  override def runJob(spark: SparkSession, lattice: LatticeContext[JourneyStageJobConfig]): Unit = {
    val config = lattice.config
    val stages = config.journeyStages.asScala
    val defaultStageName = config.defaultStage.getStageName
    val currTime = Instant.ofEpochMilli(config.currentEpochMilli)
    val priorityStageMap = stages.map(stage => (stage.getPriority, stage.getStageName)).toMap

    val timelineMaster = lattice.input(config.masterAccountTimeLineIdx)
    val timelineDiff = lattice.input(config.diffAccountTimeLineIdx)


    val priorityToStage = udf((priority: Int) => priorityStageMap(priority))

    val eventTimeCol = EventDate.getColumnName
    val mergedPriorityDf = stages.map({ stage =>
      getQualifiedAccounts(timelineMaster, stage, currTime)
        .select(AccountId.name)
        .withColumn(INTERNAL_PRIORITY_KEY, lit(stage.getPriority))
    }).reduce(_ unionByName _)
      .groupBy(AccountId.name)
      .agg(min(INTERNAL_PRIORITY_KEY).as(INTERNAL_PRIORITY_KEY))
    val mergedStageDf = mergedPriorityDf
      .withColumn(StageName.name, priorityToStage(mergedPriorityDf.col(INTERNAL_PRIORITY_KEY)))
      .withColumn(eventTimeCol, lit(currTime.toEpochMilli))
      .drop(INTERNAL_PRIORITY_KEY)

    // calculate changed journey stages and merged master journey stages
    val (masterStageDf, stageChangeDf) = if (config.masterJourneyStageIdx != null) {
      val prevAccStageDf = lattice.input(config.masterJourneyStageIdx)
      val changeDf = prevAccStageDf
        // re-generate stage events for account that does not change stage but last event is published long time ago
        .filter(prevAccStageDf.col(eventTimeCol).geq(currTime.minus(CHANGE_DETECTION_LOOKBACK_DAYS, ChronoUnit.DAYS).toEpochMilli))
        .join(mergedStageDf, Seq(AccountId.name), "outer")
        .filter(!(prevAccStageDf.col(StageName.name) <=> mergedStageDf.col(StageName.name)))
        .withColumn(INTERNAL_STAGENAME_KEY, coalesce( //
          mergedStageDf.col(StageName.name), //
          lit(defaultStageName) // previously have stage but currently not, set to default stage
        ))
        .withColumn(INTERNAL_TIME_KEY, coalesce( //
          mergedStageDf.col(eventTimeCol), //
          prevAccStageDf.col(eventTimeCol) //
        ))
        .select(AccountId.name, INTERNAL_STAGENAME_KEY, INTERNAL_TIME_KEY)
        .withColumnRenamed(INTERNAL_STAGENAME_KEY, StageName.name)
        .withColumnRenamed(INTERNAL_TIME_KEY, eventTimeCol)

      val newAccStageDf = MergeUtils.merge2(prevAccStageDf, changeDf, Seq(AccountId.name), Set(), overwriteByNull = false)
      (newAccStageDf, changeDf)
    } else {
      (mergedStageDf, mergedStageDf)
    }

    // merge changed stage
    val stageChangeEventDf = toTimelineEvent(stageChangeDf, config.accountTimeLineVersion, config.accountTimeLineId)
    val mergedMasterTimeLine = MergeUtils.concat2(timelineMaster, stageChangeEventDf)
    val mergedDiffTimeLine = MergeUtils.concat2(timelineDiff, stageChangeEventDf)

    // [ master timeline, diff timeline, master journey stage ]
    lattice.output = mergedMasterTimeLine :: mergedDiffTimeLine :: masterStageDf :: Nil
  }

  def toTimelineEvent(stageDf: DataFrame, timelineVersion: String, timelineId: String): DataFrame = {
    val generateId = udf {
      () => TimeLineStoreUtils.generateRecordId()
    }

    val generateSortKey = udf {
      (eventTimeStamp: Long, recordId: String) => TimeLineStoreUtils.generateSortKey(eventTimeStamp, recordId)
    }

    val generatePartitionKey = udf {
      accountId: String => TimeLineStoreUtils.generatePartitionKey(timelineVersion, timelineId, accountId)
    }

    val recordIdCol = RecordId.getColumnName

    val recordDf = stageDf.withColumn(recordIdCol, generateId())

    recordDf
      .withColumn(PartitionKey.name, generatePartitionKey(recordDf.col(AccountId.name)))
      .withColumn(SortKey.name, generateSortKey(recordDf.col(EventDate.getColumnName), recordDf.col(recordIdCol)))
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
