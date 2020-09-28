package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.common.exposed.util.DateTimeUtils.toDataOnlyFromMillisAndTimeZone
import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.metadata.InterfaceName._
import com.latticeengines.domain.exposed.spark.cdl.GenerateTimelineExportArtifactsJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.functions.{col, collect_list, lit, udf}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
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
    val eventTypes = config.eventTypes
    val timeZone = config.timeZone
    val accountList: DataFrame =
      if (config.accountListIdx != null) {
        lattice.input(config.accountListIdx)
      } else {
        null
      }

    val getDate = udf {
      (time: Long, timeZone: String) => toDataOnlyFromMillisAndTimeZone(time, timeZone)
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
          timelineFilterTable = timelineFilterTable.join(accountList.select(AccountId.name), Seq(AccountId.name),  "inner")
        }
        timelineFilterTable = timelineFilterTable.withColumn(Count.name, lit(1))
        if (rollupToDaily) {
          timelineFilterTable = timelineFilterTable.withColumn(__StreamDate.name, getDate(col
          (InterfaceName.EventTimestamp.name), lit(timeZone)))
          timelineFilterTable = timelineFilterTable.groupBy(AccountId.name, ContactId
            .name, EventType.name, __StreamDate.name).agg(functions.max(EventTimestamp.name).as(EventTimestamp.name),
            functions.sum(Count.name).as(Count.name), collect_list(StreamType.name).as(StreamType.name))
          timelineFilterTable = timelineFilterTable.drop(InterfaceName.__StreamDate.name)
        }
        timelineFilterTable = timelineFilterTable.drop(InterfaceName.Detail1.name).drop(InterfaceName.Detail2.name)
        timelineFilterTable = timelineFilterTable.join(latticeAccount.select(AccountId.name, DUNS.name, GlobalUltimateDuns.name,
          DomesticUltimateDuns.name, Domain.name, IsPrimaryDomain.name), Seq(AccountId.name))
        (timelineId, timelineFilterTable)
    }.toSeq: _*
    )

    val outputs = timelineExportTable.toList
    //output
    lattice.output = outputs.map(_._2)
    // timelineId -> corresponding output index
    lattice.outputStr = Serialization.write(outputs.zipWithIndex.map(t => (t._1._1, t._2)).toMap)(org.json4s.DefaultFormats)
  }
}
