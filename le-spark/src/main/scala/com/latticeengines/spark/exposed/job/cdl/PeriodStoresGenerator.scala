package com.latticeengines.spark.exposed.job.cdl

import java.util

import com.latticeengines.common.exposed.util.JsonUtils
import com.latticeengines.domain.exposed.cdl.PeriodStrategy
import com.latticeengines.domain.exposed.cdl.PeriodStrategy.Template
import com.latticeengines.domain.exposed.cdl.activity.StreamAttributeDeriver.Calculation._
import com.latticeengines.domain.exposed.cdl.activity.{AtlasStream, StreamAttributeDeriver, StreamDimension}
import com.latticeengines.domain.exposed.metadata.InterfaceName.{PeriodId, __Row_Count__, __StreamDate}
import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar
import com.latticeengines.domain.exposed.spark.cdl.ActivityStoreSparkIOMetadata.Details
import com.latticeengines.domain.exposed.spark.cdl.{ActivityStoreSparkIOMetadata, DailyStoreToPeriodStoresJobConfig}
import com.latticeengines.domain.exposed.util.TimeFilterTranslator
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.DeriveAttrsUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters.asScalaIteratorConverter

class PeriodStoresGenerator extends AbstractSparkJob[DailyStoreToPeriodStoresJobConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[DailyStoreToPeriodStoresJobConfig]): Unit = {
    val config: DailyStoreToPeriodStoresJobConfig = lattice.config
    val streams: Seq[AtlasStream] = asScalaIteratorConverter(config.streams.iterator).asScala.toSeq
    val input = lattice.input
    val inputMetadata: ActivityStoreSparkIOMetadata = config.inputMetadata
    val calendar: BusinessCalendar = config.businessCalendar

    val outputMetadata: ActivityStoreSparkIOMetadata = new ActivityStoreSparkIOMetadata()
    val detailsMap = new util.HashMap[String, Details]() // streamId -> details

    var index: Int = 0

    var periodStores: Seq[DataFrame] = Seq()
    streams.foreach((stream: AtlasStream) => {
      val dailyStore: DataFrame = input(inputMetadata.getMetadata.get(stream.getStreamId).getStartIdx)
      val generated: Seq[DataFrame] = processStream(dailyStore, stream, config.evaluationDate, calendar)
      val details: Details = new Details()
      details.setStartIdx(index)
      details.setLabels(stream.getPeriods)
      detailsMap.put(stream.getStreamId, details)

      index += generated.size
      periodStores = periodStores ++ generated
    })
    for (i <- periodStores.indices) {
      setPartitionTargets(i, Seq(PeriodId.name), lattice)
    }
    outputMetadata.setMetadata(detailsMap)
    lattice.outputStr = JsonUtils.serialize(outputMetadata)
    lattice.output = periodStores.toList
  }

  private def processStream(dailyStore: DataFrame, stream: AtlasStream, evaluationDate: String, calendar: BusinessCalendar): Seq[DataFrame] = {
    val periods: Seq[String] = asScalaIteratorConverter(stream.getPeriods.iterator).asScala.toSeq
    val dimensions: Seq[StreamDimension] = asScalaIteratorConverter(stream.getDimensions.iterator).asScala.toSeq
    val aggregators: Seq[StreamAttributeDeriver] =
      if (stream.getAttributeDerivers == null) Seq()
      else asScalaIteratorConverter(stream.getAttributeDerivers.iterator).asScala.toSeq
    val translator: TimeFilterTranslator = new TimeFilterTranslator(getPeriodStrategies(periods, calendar), evaluationDate)

    // 1: for each requested period (week, month, etc.):
    //  a: make a copy of daily store dataframe, add periodId column based on date and period
    //  b: group by (entityId + all dimensions from stream + periodId), apply aggregations on target attributes as marked by attribute derivers
    // 2: return all dataframes

    val withPeriodId: Seq[DataFrame] = periods.map(periodName => generatePeriodId(dailyStore, periodName, translator))

    val aggrProcs = aggregators.map(aggr => {
      val targetAttr = aggr.getTargetAttribute
      (aggr.getCalculation match {
        case SUM => sum(dailyStore(targetAttr))
        case MIN => min(dailyStore(targetAttr))
        case MAX => max(dailyStore(targetAttr))
        case _ => throw new UnsupportedOperationException("Unsupported operation")
      }).alias(targetAttr)
    }) :+ sum(dailyStore(__Row_Count__.name)).alias(__Row_Count__.name) // Default row count must exist
    val columns: Seq[String] = DeriveAttrsUtils.getGroupByEntityColsFromStream(stream) ++ (dimensions.map(_.getName) :+ PeriodId.name)
    val groupedByPeriodId: Seq[DataFrame] = withPeriodId.map((df: DataFrame) => df.groupBy(columns.head, columns.tail: _*)
      .agg(aggrProcs.head, aggrProcs.tail: _*))
    groupedByPeriodId.map((df: DataFrame) => DeriveAttrsUtils.appendPartitionColumns(df, Seq(PeriodId.name)))
  }

  private def generatePeriodId(dailyStore: DataFrame, periodName: String, translator: TimeFilterTranslator): DataFrame = {

    def getPeriodIdFunc: String => Int = (dateStr: String) => translator.dateToPeriod(periodName, dateStr)

    def getPeriodIdUdf = UserDefinedFunction(getPeriodIdFunc, IntegerType, Some(Seq(StringType)))

    dailyStore.withColumn(PeriodId.name, getPeriodIdUdf(dailyStore(__StreamDate.name)))
  }

  private def toPeriodStrategy(name: String, calendar: BusinessCalendar): PeriodStrategy = {
    new PeriodStrategy(calendar, Template.fromName(name))
  }

  private def getPeriodStrategies(periods: Seq[String], calendar: BusinessCalendar): util.List[PeriodStrategy] = {
    scala.collection.JavaConversions.seqAsJavaList(periods.map(name => toPeriodStrategy(name, calendar)))
  }
}
