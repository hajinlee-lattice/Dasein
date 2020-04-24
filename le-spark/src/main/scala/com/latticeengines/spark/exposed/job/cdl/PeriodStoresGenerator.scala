package com.latticeengines.spark.exposed.job.cdl

import java.util

import com.latticeengines.domain.exposed.cdl.PeriodStrategy
import com.latticeengines.domain.exposed.cdl.PeriodStrategy.Template
import com.latticeengines.domain.exposed.cdl.activity.{AtlasStream, StreamAttributeDeriver, StreamDimension}
import com.latticeengines.domain.exposed.metadata.InterfaceName.{LastActivityDate, PeriodId, __Row_Count__, __StreamDate}
import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar
import com.latticeengines.domain.exposed.spark.cdl.ActivityStoreSparkIOMetadata.Details
import com.latticeengines.domain.exposed.spark.cdl.{ActivityStoreSparkIOMetadata, DailyStoreToPeriodStoresJobConfig}
import com.latticeengines.domain.exposed.util.TimeFilterTranslator
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.{DeriveAttrsUtils, MergeUtils}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.JavaConversions._

class PeriodStoresGenerator extends AbstractSparkJob[DailyStoreToPeriodStoresJobConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[DailyStoreToPeriodStoresJobConfig]): Unit = {
    val config: DailyStoreToPeriodStoresJobConfig = lattice.config
    val streams: Seq[AtlasStream] = config.streams.toSeq
    val input = lattice.input
    val inputMetadata: ActivityStoreSparkIOMetadata = config.inputMetadata
    val calendar: BusinessCalendar = config.businessCalendar
    val incrementalStreams = config.incrementalStreams

    val outputMetadata: ActivityStoreSparkIOMetadata = new ActivityStoreSparkIOMetadata()
    val detailsMap = new util.HashMap[String, Details]() // streamId -> details
    var periodStores: Seq[DataFrame] = Seq()
    streams.foreach(stream => {
      val streamId: String = stream.getStreamId
      val inputDetails = inputMetadata.getMetadata.get(streamId)
      val outputDetails: Details = new Details()
      outputDetails.setStartIdx(periodStores.size)
      outputDetails.setLabels(stream.getPeriods)
      detailsMap.put(stream.getStreamId, outputDetails)
      if (incrementalStreams.contains(stream.getStreamId)) {
        val dailyStoreDelta: DataFrame = input(inputDetails.getStartIdx + inputDetails.getLabels.size) // last one in stream's section
        // get period store delta tables in same order as stream periods list
        val periodStoreDelta: Seq[DataFrame] = processDailyStore(dailyStoreDelta, stream, config.evaluationDate, calendar)
        val updatedPeriodStore: Seq[DataFrame] = updatePeriodStoreBatch(stream, periodStoreDelta, lattice)
        periodStores = periodStores ++ updatedPeriodStore
      } else {
        periodStores = periodStores ++ processDailyStore(input(inputDetails.getStartIdx), stream, config.evaluationDate, calendar)
      }
    })
    outputMetadata.setMetadata(detailsMap)
    for (index <- periodStores.indices) {
      setPartitionTargets(index, Seq(PeriodId.name), lattice)
    }
    lattice.output = periodStores.toList
    lattice.outputStr = serializeJson(outputMetadata)
  }

  def updatePeriodStoreBatch(stream: AtlasStream, periodStoreDelta: Seq[DataFrame], lattice: LatticeContext[DailyStoreToPeriodStoresJobConfig]): Seq[DataFrame] = {
    if (lattice.config.streamsWithNoBatch.contains(stream.getStreamId)) {
      periodStoreDelta
    } else {
      var updatedPeriodStore: Seq[DataFrame] = Seq()
      val inputDetails = lattice.config.inputMetadata.getMetadata.get(stream.getStreamId)
      var offset: Int = 0
      periodStoreDelta.foreach(deltaTable => {
        val periodBatch: DataFrame = lattice.input(inputDetails.getStartIdx + offset)
        updatedPeriodStore :+= updatePeriodBatchWithDelta(periodBatch, deltaTable, stream)
        offset += 1
      })
      updatedPeriodStore
    }
  }

  def updatePeriodBatchWithDelta(periodBatch: DataFrame, deltaTable: DataFrame, stream: AtlasStream): DataFrame = {
    val affectedPeriods = deltaTable.select(PeriodId.name).distinct.rdd.map(r => r(0).asInstanceOf[Int]).collect()
    val columns: Seq[String] = DeriveAttrsUtils.getEntityIdColsFromStream(stream) ++ (stream.getDimensions.map(_.getName) :+ PeriodId.name)

    val updatedSubTable: DataFrame = MergeUtils.concat2(periodBatch.filter(col(PeriodId.name).isInCollection(affectedPeriods)), deltaTable)
      .groupBy(columns.head, columns.tail: _*)
      .agg(sum(__Row_Count__.name).as(__Row_Count__.name), max(LastActivityDate.name).as(LastActivityDate.name))
    MergeUtils.concat2(
      DeriveAttrsUtils.appendPartitionColumns(updatedSubTable, Seq(PeriodId.name)),
      periodBatch.filter(!periodBatch(PeriodId.name).isInCollection(affectedPeriods))
    )
  }

  private def processDailyStore(dailyStore: DataFrame, stream: AtlasStream, evaluationDate: String, calendar: BusinessCalendar): Seq[DataFrame] = {
    val periods: Seq[String] = stream.getPeriods.toSeq
    val dimensions: Seq[StreamDimension] = stream.getDimensions.toSeq
    val aggregations: Seq[StreamAttributeDeriver] =
      if (Option(stream.getAttributeDerivers).isEmpty) Seq()
      else stream.getAttributeDerivers.toSeq
    val translator: TimeFilterTranslator = new TimeFilterTranslator(getPeriodStrategies(periods, calendar), evaluationDate)

    // for each requested period name (week, month, etc.):
    //  a: append periodId column based on date and period name
    //  b: apply reducer if defined
    //  c: group to period: group by (entityId + all dimensions from stream + periodId) unless reducer is defined

    var withPeriodId: Seq[DataFrame] = periods.map(periodName => generatePeriodId(dailyStore, periodName, translator, stream))
    if (Option(stream.getReducer).isEmpty) { // if reducer exists, no need to group by PeriodId as each periodId should only has one record
      // Default row count and last activity date must exist
      val defaultAggColumns = Seq(sum(__Row_Count__.name).as(__Row_Count__.name), max(LastActivityDate.name).as(LastActivityDate.name))
      val aggColumns: Seq[Column] = aggregations.map(aggr => DeriveAttrsUtils.getAggr(dailyStore, aggr)) ++ defaultAggColumns
      val columns: Seq[String] = DeriveAttrsUtils.getEntityIdColsFromStream(stream) ++ (dimensions.map(_.getName) :+ PeriodId.name)
      withPeriodId = withPeriodId.map((df: DataFrame) =>
        addLastActivityDateColIfNotExist(df).groupBy(columns.head, columns.tail: _*).agg(aggColumns.head, aggColumns.tail: _*))
    }
    withPeriodId.map((df: DataFrame) => DeriveAttrsUtils.appendPartitionColumns(df, Seq(PeriodId.name)))
  }

  private def generatePeriodId(dailyStore: DataFrame, periodName: String, translator: TimeFilterTranslator, stream: AtlasStream): DataFrame = {

    def getPeriodIdFunc: String => Int = (dateStr: String) => translator.dateToPeriod(periodName, dateStr)

    def getPeriodIdUdf = UserDefinedFunction(getPeriodIdFunc, IntegerType, Some(Seq(StringType)))

    var df = dailyStore.withColumn(PeriodId.name, getPeriodIdUdf(dailyStore(__StreamDate.name)))

    if (Option(stream.getReducer).isDefined) {
      val reducer = Option(stream.getReducer).get

      if (DeriveAttrsUtils.isTimeReducingOperation(reducer.getOperator)) {
        reducer.getGroupByFields.append(PeriodId.name) // separate by each period if filter is applied for time
      }
      df = DeriveAttrsUtils.applyReducer(df, reducer)
    }
    df
  }

  private def toPeriodStrategy(name: String, calendar: BusinessCalendar): PeriodStrategy = {
    new PeriodStrategy(calendar, Template.fromName(name))
  }

  private def getPeriodStrategies(periods: Seq[String], calendar: BusinessCalendar): util.List[PeriodStrategy] = {
    scala.collection.JavaConversions.seqAsJavaList(periods.map(name => toPeriodStrategy(name, calendar)))
  }

  private def addLastActivityDateColIfNotExist(df: DataFrame): DataFrame = {
    if (!df.columns.contains(LastActivityDate.name)) {
      df.withColumn(LastActivityDate.name, lit(null).cast(LongType))
    } else {
      df
    }
  }
}
