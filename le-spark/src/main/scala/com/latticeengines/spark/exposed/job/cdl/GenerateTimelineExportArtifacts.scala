package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.common.exposed.util.DateTimeUtils.{toDateOnlyFromMillisAndTimeZone, toDateSecondFromMillisAndTimeZone}
import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.metadata.InterfaceName._
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit
import com.latticeengines.domain.exposed.spark.cdl.GenerateTimelineExportArtifactsJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.CSVUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.jackson.Serialization

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.immutable

class GenerateTimelineExportArtifacts extends AbstractSparkJob[GenerateTimelineExportArtifactsJobConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[GenerateTimelineExportArtifactsJobConfig]): Unit = {

    val config: GenerateTimelineExportArtifactsJobConfig = lattice.config
    val inputIdx = config.inputIdx
    val timelineTableNames = config.timelineTableNames.asScala
    val latticeAccount: DataFrame = lattice.input(config.latticeAccountTableIdx)
    val fromDateTimestamp = config.fromDateTimestamp
    val toDateTimestamp = config.toDateTimestamp
    val rollupToDaily = config.rollupToDaily
    val filterDuns = config.filterDuns
    val includeOrphan = config.includeOrphan
    val eventTypes = config.eventTypes
    val timeZone = config.timeZone
    val accountList: DataFrame =
      if (config.accountListIdx != null) {
        lattice.input(config.accountListIdx)
      } else {
        null
      }

    val getDate = udf {
      (time: Long, timeZone: String) => toDateOnlyFromMillisAndTimeZone(time, timeZone)
    }

    val getDateSecond = udf {
      (time: Long, timeZone: String) => toDateSecondFromMillisAndTimeZone(time, timeZone)
    }

    val timelineExportTable = immutable.Map(timelineTableNames.map{
      case (timelineId, timelineTableName) =>
        val timelineTable = lattice.input(inputIdx.get(timelineTableName))
        var timelineFilterTable = timelineTable
        timelineFilterTable = timelineFilterTable.drop(Id.name)
        if (fromDateTimestamp != null) {
          timelineFilterTable = timelineFilterTable.filter(col(EventTimestamp.name) >=
            fromDateTimestamp)
        }
        if (toDateTimestamp != null) {
          timelineFilterTable = timelineFilterTable.filter(col(EventTimestamp.name) <= toDateTimestamp)
        }
        if (eventTypes != null && eventTypes.size() > 0) {
          timelineFilterTable = timelineFilterTable.filter(col(EventType.name).isInCollection
          (eventTypes))
        }
        if (accountList != null) {
          timelineFilterTable = timelineFilterTable.join(accountList.select(AccountId.name), Seq(AccountId.name))
        }
        timelineFilterTable = timelineFilterTable.withColumn(Count.name, lit(1))
        if (rollupToDaily) {
          timelineFilterTable = timelineFilterTable
            .withColumn(EventDate.name, getDate(col(EventTimestamp.name), lit(timeZone)))
            .groupBy(AccountId.name, ContactId.name, StreamType.name, EventType.name, EventDate.name)
            .agg(sum(Count.name).as(Count.name))
        } else {
          // TODO consider timezone and maybe consider other format
          timelineFilterTable = timelineFilterTable
            .withColumn(EventDate.name, getDateSecond(col(EventTimestamp.name), lit(timeZone)))
        }
        var joinType = "inner"
        if (includeOrphan) {
          joinType = "left"
        }
        timelineFilterTable = timelineFilterTable.select(col(AccountId.name), col(ContactId.name), col(EventDate
          .name), col(EventType.name), col(StreamType.name), col(Count.name))
        timelineFilterTable = timelineFilterTable.join(latticeAccount.select(AccountId.name, "LDC_DUNS",
          "DOMESTIC_ULTIMATE_DUNS_NUMBER",
          "GLOBAL_ULTIMATE_DUNS_NUMBER", "LDC_DOMAIN", "LE_IS_PRIMARY_DOMAIN"), Seq(AccountId.name), joinType)
          .withColumnRenamed("LDC_DUNS", DUNS.name).withColumnRenamed("DOMESTIC_ULTIMATE_DUNS_NUMBER",
          DomesticUltimateDuns.name).withColumnRenamed("GLOBAL_ULTIMATE_DUNS_NUMBER", GlobalUltimateDuns.name)
          .withColumnRenamed("LDC_DOMAIN", Domain.name).withColumnRenamed("LE_IS_PRIMARY_DOMAIN", IsPrimaryDomain.name)
        if (filterDuns) {
          timelineFilterTable = timelineFilterTable.filter(col(DUNS.name()).isNotNull)
        }
        (timelineId, timelineFilterTable)
    }.toSeq: _*
    )

    val outputs = timelineExportTable.toList
    //output
    lattice.output = outputs.map(_._2)
    // timelineId -> corresponding output index
    lattice.outputStr = Serialization.write(outputs.zipWithIndex.map(t => (t._1._1, t._2)).toMap)(org.json4s.DefaultFormats)
  }

  override def finalizeJob(spark: SparkSession, latticeCtx: LatticeContext[GenerateTimelineExportArtifactsJobConfig]): List[HdfsDataUnit] = {
    CSVUtils.dfToCSV(spark, compress = false, latticeCtx.targets, latticeCtx.output)
  }
}
