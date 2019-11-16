package com.latticeengines.spark.exposed.job.cdl

import java.util

import com.latticeengines.common.exposed.util.TemplateUtils
import com.latticeengines.domain.exposed.StringTemplates
import com.latticeengines.domain.exposed.cdl.PeriodStrategy
import com.latticeengines.domain.exposed.cdl.PeriodStrategy.Template
import com.latticeengines.domain.exposed.cdl.activity.StreamAttributeDeriver.Calculation
import com.latticeengines.domain.exposed.cdl.activity.{ActivityMetricsGroup, ActivityMetricsGroupUtils, ActivityTimeRange}
import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.metadata.transaction.NullMetricsImputation.{NULL, ZERO}
import com.latticeengines.domain.exposed.query.{ComparisonType, TimeFilter}
import com.latticeengines.domain.exposed.spark.cdl.DeriveActivityMetricGroupJobConfig
import com.latticeengines.domain.exposed.util.TimeFilterTranslator
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.DeriveAttrsUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.JavaConverters.asScalaIteratorConverter

class MetricsGroupGenerator extends AbstractSparkJob[DeriveActivityMetricGroupJobConfig] {
  // generate a group of metrics based on ActivityMetricsGroup definition (aggregated to defined business entity)

  private val TMPLKEY_GROUPID = "GroupId";
  private val TMPLKEY_ROLLUP_DIM_IDs = "RollupDimIds";
  private val TMPLKEY_TIMERANGE = "TimeRange";

  override def runJob(spark: SparkSession, lattice: LatticeContext[DeriveActivityMetricGroupJobConfig]): Unit = {
    val config: DeriveActivityMetricGroupJobConfig = lattice.config
    val evaluationDate = config.evaluationDate // yyyy-mm-dd
    val periodStores = config.periodStoreMap
    val input: Seq[DataFrame] = lattice.input
    val groupConfig: ActivityMetricsGroup = config.activityMetricsGroup
    val translator: TimeFilterTranslator = new TimeFilterTranslator(getPeriodStrategies(groupConfig.getActivityTimeRange), evaluationDate)

    // run defined aggregation on period stores, remove null entries that have no meaning
    val aggregatedPeriodStores: Seq[DataFrame] = input.map(df => df.na.drop)

    // divide dataframe by time filters defined in TimeRange SQL column
    val filteredByTime: Seq[(DataFrame, String)] = createTimeFilters(groupConfig.getActivityTimeRange)
      .map(tf => separateByTimeFilter(aggregatedPeriodStores(periodStores.get(tf.getPeriod)), tf, translator))

    // rollup by (entityId, rollupDimensions), pivot and rename pivoted attribute following attrName template
    val attrRolledUp: Seq[DataFrame] = filteredByTime.map(item => rollupAndCreateAttr(item._1, item._2, groupConfig))

    // join dataframes from all time filters
    val entityIdColName = DeriveAttrsUtils.getMetricsGroupEntityIdColumnName(groupConfig.getEntity)
    val joined: DataFrame = attrRolledUp.reduce((df1, df2) => {
      df1.join(df2, Seq(entityIdColName), "fullouter")
    })

    // replace null values with defined method
    val replaceNull: DataFrame = groupConfig.getNullImputation match {
      case NULL => joined // no operation needed
      case ZERO => joined.na.fill(0) // fill with 0
      case _ => throw new UnsupportedOperationException("Unknown null imputation method")
    }

    lattice.output = replaceNull :: Nil
  }

  // return: (dataframe, TimeRange string used for attribute name template)
  def separateByTimeFilter(df: DataFrame, timeFilter: TimeFilter, translator: TimeFilterTranslator): (DataFrame, String) = {
    val periodIdColumnName: String = InterfaceName.PeriodId.name()
    val bounds = translator.translateRange(timeFilter)

    val timeRangeStr: String = ActivityMetricsGroupUtils.timeFilterToTimeRangeTemplate(timeFilter)

    (df.filter(df(periodIdColumnName).between(bounds.getLeft, bounds.getRight)), timeRangeStr)
  }

  def rollupAndCreateAttr(df: DataFrame, timeRangeName: String, groupConfig: ActivityMetricsGroup): DataFrame = {
    val targetAttr: String = groupConfig.getAggregation.getTargetAttribute
    val entityIdColName: String = DeriveAttrsUtils.getMetricsGroupEntityIdColumnName(groupConfig.getEntity)
    val rollupDimNames: Seq[String] = groupConfig.getRollupDimensions.split(",") :+ entityIdColName
    val pivotDimNames: Seq[String] = groupConfig.getRollupDimensions.split(",")
    val pivotCols: Seq[String] = df.columns.filter(colName => pivotDimNames.contains(colName)).toSeq
    val calc = groupConfig.getAggregation.getCalculation

    val rollupAggr = (calc match {
      case Calculation.SUM => sum(df(targetAttr))
      case Calculation.MAX => max(df(targetAttr))
      case Calculation.MIN => min(df(targetAttr))
      case _ => throw new UnsupportedOperationException("Unsupported operation")
    }).alias(targetAttr)

    val rolledUp: DataFrame = df.rollup(rollupDimNames.head, rollupDimNames.tail: _*).agg(rollupAggr)

    val excludeNull: DataFrame = rolledUp.where(rolledUp(entityIdColName).isNotNull && rolledUp(targetAttr).isNotNull)

    def concatFunc: Row => String = (row: Row) => row.mkString(",")

    def combineUdf: UserDefinedFunction = udf(concatFunc)

    val pivotAggr = (calc match {
      case Calculation.SUM => sum(excludeNull(targetAttr))
      case Calculation.MIN => min(excludeNull(targetAttr))
      case Calculation.MAX => max(excludeNull(targetAttr))
      case _ => throw new UnsupportedOperationException("Unsupported operation")
    }).alias(targetAttr)
    val pivoted: DataFrame = excludeNull.withColumn("combColumn", combineUdf(struct(pivotCols.map(col): _*))).groupBy(entityIdColName)
      .pivot("combColumn").agg(pivotAggr)

    val attrRenamed: DataFrame = pivoted.columns.foldLeft(pivoted) { (pivotedDF, colName) =>
      if (!colName.equals(entityIdColName)) {
        val rollupDimVals: Seq[String] = colName.split(",")
        val attrName: String = constructAttrName(groupConfig.getGroupId, rollupDimVals, timeRangeName)
        pivotedDF.withColumnRenamed(colName, attrName)
      } else {
        pivotedDF
      }
    }
    attrRenamed
  }

  // create time filters based on timeRange defined
  private def createTimeFilters(timeRange: ActivityTimeRange): Seq[TimeFilter] = {
    val periods: Seq[String] = asScalaIteratorConverter(timeRange.getPeriods.iterator).asScala.toSeq
    val paramSet: Seq[util.List[Integer]] = asScalaIteratorConverter(timeRange.getParamSet.iterator).asScala.toSeq
    val timeFilters: Seq[TimeFilter] = timeRange.getOperator match {
      case ComparisonType.WITHIN => for (period <- periods; params <- paramSet) yield TimeFilter.within(params.get(0), period)
      case _ => throw new UnsupportedOperationException("Only support time filter WITHIN operation")
    }
    timeFilters
  }

  private def constructAttrName(groupId: String, rollupDimIds: Seq[Object], timeRangeStr: String): String = {
    val map: util.Map[String, Object] = new util.HashMap[String, Object]()
    map.put(TMPLKEY_GROUPID, groupId)
    map.put(TMPLKEY_ROLLUP_DIM_IDs, seqAsJavaList(rollupDimIds))
    map.put(TMPLKEY_TIMERANGE, timeRangeStr)
    TemplateUtils.renderByMap(StringTemplates.ACTIVITY_METRICS_GROUP_ATTRNAME, map).toLowerCase()
  }

  private def toPeriodStrategy(name: String): PeriodStrategy = {
    new PeriodStrategy(Template.fromName(name))
  }

  private def getPeriodStrategies(timeRange: ActivityTimeRange): util.List[PeriodStrategy] = {
    scala.collection.JavaConversions.seqAsJavaList(
      asScalaIteratorConverter(timeRange.getPeriods.iterator).asScala.toSeq.map(name => toPeriodStrategy(name))
    )
  }
}
