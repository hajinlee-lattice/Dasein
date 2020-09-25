package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.common.exposed.util.DateTimeUtils.toDateOnlyFromMillis
import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.metadata.InterfaceName.{AccountId, ContactId, DUNS, Domain, EventTimestamp, EventType, __StreamDate}
import com.latticeengines.domain.exposed.spark.cdl.ExportTimelineJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.json4s.jackson.Serialization

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.immutable

class ExportTimelineJob extends AbstractSparkJob[ExportTimelineJobConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[ExportTimelineJobConfig]): Unit = {

    val config: ExportTimelineJobConfig = lattice.config
    val inputIdx = config.inputIdx
    val timelineTableNames = config.timelineTableNames.asScala
    val latticeAccount: DataFrame = lattice.input(config.latticeAccountTableIdx)
    val fromDateTimestamp = config.fromDateTimestamp
    val toDateTimestamp = config.toDateTimestamp
    val rollupToDaily = config.rollupToDaily
    val eventTypes = config.eventTypes
    val countColumn = "Count"
    val accountList: DataFrame =
      if (config.timelineUniverseAccountListIdx != null) {
        lattice.input(config.timelineUniverseAccountListIdx)
      } else {
        null
      }

    val getDate = udf {
      time: Long => toDateOnlyFromMillis(time.toString)
    }

    val timelineExportTable = immutable.Map(timelineTableNames.map{
      case (timelineId, timelineTableName) =>
        val timelineTable = lattice.input(inputIdx.get(timelineTableName))
        var timelineFilterTable = timelineTable
        if (fromDateTimestamp != null) {
          timelineFilterTable = timelineFilterTable.where(col(EventTimestamp.name) >=
            fromDateTimestamp)
        }
        if (toDateTimestamp != null) {
          timelineFilterTable = timelineFilterTable.where(col(EventTimestamp.name) <= toDateTimestamp)
        }
        if (eventTypes != null && eventTypes.size() > 0) {
          timelineFilterTable = timelineFilterTable.filter(col(EventType.name).isInCollection
          (eventTypes))
        }
        if (accountList != null) {
          timelineFilterTable = timelineFilterTable.join(accountList.select(AccountId.name), Seq(AccountId.name),  "inner")
        }
        timelineFilterTable = timelineFilterTable.withColumn(countColumn, lit(1))
        if (rollupToDaily) {
          timelineFilterTable = timelineFilterTable.withColumn(__StreamDate.name, getDate(col
          (InterfaceName.EventTimestamp.name)))
          timelineFilterTable = timelineFilterTable.groupBy(AccountId.name, ContactId
            .name, EventType.name, __StreamDate.name).agg(functions.max(EventTimestamp.name).as(EventTimestamp.name),
            functions.sum(countColumn).as(countColumn))
          timelineFilterTable.drop(__StreamDate.name)
        }
        timelineFilterTable = timelineFilterTable.join(latticeAccount.select(AccountId.name, DUNS.name(), "DU_DUNS",
          "GU_DUNS", Domain.name(), "IsPrimaryDomain"), Seq(AccountId.name))
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