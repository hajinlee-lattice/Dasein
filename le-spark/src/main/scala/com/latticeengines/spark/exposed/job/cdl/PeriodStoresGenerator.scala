package com.latticeengines.spark.exposed.job.cdl

import java.util

import com.latticeengines.domain.exposed.cdl.PeriodStrategy
import com.latticeengines.domain.exposed.cdl.PeriodStrategy.Template
import com.latticeengines.domain.exposed.cdl.activity.StreamAttributeDeriver.Calculation._
import com.latticeengines.domain.exposed.cdl.activity.{AtlasStream, StreamAttributeDeriver, StreamDimension}
import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.query.BusinessEntity
import com.latticeengines.domain.exposed.spark.cdl.DailyStoreToPeriodStoresJobConfig
import com.latticeengines.domain.exposed.util.TimeFilterTranslator
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters.asScalaIteratorConverter

class PeriodStoresGenerator extends AbstractSparkJob[DailyStoreToPeriodStoresJobConfig] {

  val PeriodId: String = InterfaceName.PeriodId.name
  val PeriodIdForPartition: String = InterfaceName.PeriodId.name + "Partition"

  override def runJob(spark: SparkSession, lattice: LatticeContext[DailyStoreToPeriodStoresJobConfig]): Unit = {

    val config: DailyStoreToPeriodStoresJobConfig = lattice.config
    val dailyStore: DataFrame = lattice.input.head
    val stream: AtlasStream = config.stream
    val evaluationDate: String = config.evaluationDate
    val periods: Seq[String] = asScalaIteratorConverter(stream.getPeriods.iterator).asScala.toSeq
    val dimensions: Seq[StreamDimension] = asScalaIteratorConverter(stream.getDimensions.iterator).asScala.toSeq
    val aggregators: Seq[StreamAttributeDeriver] = asScalaIteratorConverter(stream.getAttributeDerivers.iterator).asScala.toSeq
    val translator: TimeFilterTranslator = new TimeFilterTranslator(getPeriodStrategies(periods), evaluationDate)

    // parase date ranges:
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
    val columns: Seq[String] = getEntityIdColName(stream) ++ (dimensions.map(_.getName) :+ PeriodId)
    val groupedByPeriodId: Seq[DataFrame] = withPeriodId.map((df: DataFrame) => df.groupBy(columns.head, columns.tail: _*)
      .agg(aggrProcs.head, aggrProcs.tail: _*)
      .withColumn(PeriodIdForPartition, col(PeriodId))) // duplicate column for functional test

    for (i <- groupedByPeriodId.indices) {
      setPartitionTargets(i, Seq(PeriodId), lattice)
    }

    lattice.output = groupedByPeriodId.toList
  }

  private def generatePeriodId(dailyStore: DataFrame, periodName: String, translator: TimeFilterTranslator): DataFrame = {

    def getPeriodIdFunc: String => Int = (dateStr: String) => translator.dateToPeriod(periodName, dateStr)

    def getPeriodIdUdf = UserDefinedFunction(getPeriodIdFunc, IntegerType, Some(Seq(StringType)))

    dailyStore.withColumn(PeriodId, getPeriodIdUdf(dailyStore("Date")))
  }

  private def toPeriodStrategy(name: String): PeriodStrategy = {
    new PeriodStrategy(Template.fromName(name))
  }

  private def getPeriodStrategies(periods: Seq[String]): util.List[PeriodStrategy] = {
    scala.collection.JavaConversions.seqAsJavaList(periods.map(name => toPeriodStrategy(name)))
  }

  private def getEntityIdColName(stream: AtlasStream): Seq[String] = {
    if (stream.getAggrEntities.contains(BusinessEntity.Contact.name)) {
      Seq(InterfaceName.AccountId.name, InterfaceName.ContactId.name)
    } else {
      Seq(InterfaceName.AccountId.name)
    }
  }
}
