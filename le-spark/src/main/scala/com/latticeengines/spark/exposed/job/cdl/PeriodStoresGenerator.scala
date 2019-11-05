package com.latticeengines.spark.exposed.job.cdl

import java.util

import com.latticeengines.common.exposed.util.JsonUtils
import com.latticeengines.domain.exposed.cdl.PeriodStrategy
import com.latticeengines.domain.exposed.cdl.PeriodStrategy.Template
import com.latticeengines.domain.exposed.cdl.activity.StreamAttributeDeriver.Calculation._
import com.latticeengines.domain.exposed.cdl.activity.{AtlasStream, StreamAttributeDeriver, StreamDimension}
import com.latticeengines.domain.exposed.metadata.InterfaceName.{AccountId, ContactId, PeriodId, __StreamDate}
import com.latticeengines.domain.exposed.query.BusinessEntity
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

  private val PeriodIdForPartition: String = PeriodId.name + "Partition"

  override def runJob(spark: SparkSession, lattice: LatticeContext[DailyStoreToPeriodStoresJobConfig]): Unit = {
    val config: DailyStoreToPeriodStoresJobConfig = lattice.config
    val streams: Seq[AtlasStream] = asScalaIteratorConverter(config.streams.iterator).asScala.toSeq
    val input = lattice.input
    val inputMetadata: ActivityStoreSparkIOMetadata = config.inputMetadata

    val outputMetadata: ActivityStoreSparkIOMetadata = new ActivityStoreSparkIOMetadata()
    val detailsMap = new util.HashMap[String, Details]() // streamId -> details

    var index: Int = 0

    var periodStores: Seq[DataFrame] = Seq()
    streams.foreach((stream: AtlasStream) => {
      val dailyStore: DataFrame = input(inputMetadata.getMetadata.get(stream.getStreamId).getStartIdx)
      val generated: Seq[DataFrame] = processStream(dailyStore, stream, config.evaluationDate)
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

  private def processStream(dailyStore: DataFrame, stream: AtlasStream, evaluationDate: String): Seq[DataFrame] = {
    val periods: Seq[String] = asScalaIteratorConverter(stream.getPeriods.iterator).asScala.toSeq
    val dimensions: Seq[StreamDimension] = asScalaIteratorConverter(stream.getDimensions.iterator).asScala.toSeq
    val aggregators: Seq[StreamAttributeDeriver] = asScalaIteratorConverter(stream.getAttributeDerivers.iterator).asScala.toSeq
    val translator: TimeFilterTranslator = new TimeFilterTranslator(getPeriodStrategies(periods), evaluationDate)

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
    })
    val columns: Seq[String] = DeriveAttrsUtils.getGroupByEntityColsFromStream(stream) ++ (dimensions.map(_.getName) :+ PeriodId.name)
    val groupedByPeriodId: Seq[DataFrame] = withPeriodId.map((df: DataFrame) => df.groupBy(columns.head, columns.tail: _*)
      .agg(aggrProcs.head, aggrProcs.tail: _*).withColumn(PeriodIdForPartition, df(PeriodId.name)))
    groupedByPeriodId
  }

  private def generatePeriodId(dailyStore: DataFrame, periodName: String, translator: TimeFilterTranslator): DataFrame = {

    def getPeriodIdFunc: String => Int = (dateStr: String) => translator.dateToPeriod(periodName, dateStr)

    def getPeriodIdUdf = UserDefinedFunction(getPeriodIdFunc, IntegerType, Some(Seq(StringType)))

    dailyStore.withColumn(PeriodId.name, getPeriodIdUdf(dailyStore(__StreamDate.name)))
  }

  private def toPeriodStrategy(name: String): PeriodStrategy = {
    new PeriodStrategy(Template.fromName(name))
  }

  private def getPeriodStrategies(periods: Seq[String]): util.List[PeriodStrategy] = {
    scala.collection.JavaConversions.seqAsJavaList(periods.map(name => toPeriodStrategy(name)))
  }
}
