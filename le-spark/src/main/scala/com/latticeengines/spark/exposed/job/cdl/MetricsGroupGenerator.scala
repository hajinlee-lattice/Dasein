package com.latticeengines.spark.exposed.job.cdl

import java.util

import com.latticeengines.common.exposed.util.TemplateUtils
import com.latticeengines.domain.exposed.StringTemplateConstants
import com.latticeengines.domain.exposed.cdl.PeriodStrategy
import com.latticeengines.domain.exposed.cdl.PeriodStrategy.Template
import com.latticeengines.domain.exposed.cdl.activity._
import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.metadata.transaction.NullMetricsImputation.{NULL, ZERO, FALSE}
import com.latticeengines.domain.exposed.query.{BusinessEntity, TimeFilter}
import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar
import com.latticeengines.domain.exposed.spark.cdl.ActivityStoreSparkIOMetadata.Details
import com.latticeengines.domain.exposed.spark.cdl.{ActivityStoreSparkIOMetadata, DeriveActivityMetricGroupJobConfig}
import com.latticeengines.domain.exposed.util.TimeFilterTranslator
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.DeriveAttrsUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConversions._

class MetricsGroupGenerator extends AbstractSparkJob[DeriveActivityMetricGroupJobConfig] {
  // generate a group of metrics based on ActivityMetricsGroup definition (aggregated to defined business entity)

  private val TMPLKEY_GROUPID = "GroupId"
  private val TMPLKEY_ROLLUP_DIM_IDs = "RollupDimIds"
  private val TMPLKEY_TIMERANGE = "TimeRange"
  private val ACCOUNT_BATCH_STORE = BusinessEntity.Account.name
  private val CONTACT_BATCH_STORE = BusinessEntity.Contact.name

  private var hasAccountBatchStore: Boolean = false
  private var hasContactBatchStore: Boolean = false
  private var accountBatchStoreTable: DataFrame = _
  private var contactBatchStoreTable: DataFrame = _

  override def runJob(spark: SparkSession, lattice: LatticeContext[DeriveActivityMetricGroupJobConfig]): Unit = {
    import spark.implicits._
    val config: DeriveActivityMetricGroupJobConfig = lattice.config
    val evaluationDate = config.evaluationDate // yyyy-mm-dd
    val calendar = config.businessCalendar
    val input: Seq[DataFrame] = lattice.input
    val groups: Seq[ActivityMetricsGroup] = config.activityMetricsGroups.toSeq
    val translator: TimeFilterTranslator = new TimeFilterTranslator(getPeriodStrategies(groups, calendar), evaluationDate)
    val inputMetadata: ActivityStoreSparkIOMetadata = config.inputMetadata
    val streamMetadata = config.streamMetadataMap
    hasAccountBatchStore = inputMetadata.getMetadata.contains(ACCOUNT_BATCH_STORE)
    hasContactBatchStore = inputMetadata.getMetadata.contains(CONTACT_BATCH_STORE)

    var aggregatedPeriodStores: Seq[DataFrame] = input.map(df => DeriveAttrsUtils.dropPartitionColumns(df))

    // exclude account and contact batch store
    if (hasAccountBatchStore && hasContactBatchStore) {
      aggregatedPeriodStores = aggregatedPeriodStores.dropRight(2)
    } else if (hasAccountBatchStore || hasContactBatchStore) {
      aggregatedPeriodStores = aggregatedPeriodStores.dropRight(1)
    }
    if (hasAccountBatchStore) {
      accountBatchStoreTable = input.get(inputMetadata.getMetadata.get(ACCOUNT_BATCH_STORE).getStartIdx)
    }
    if (hasContactBatchStore) {
      contactBatchStoreTable = input.get(inputMetadata.getMetadata.get(CONTACT_BATCH_STORE).getStartIdx)
    }

    val outputMetadata: ActivityStoreSparkIOMetadata = new ActivityStoreSparkIOMetadata()
    val detailsMap = new util.HashMap[String, Details]() // groupId -> details
    var index: Int = 0
    var metrics: Seq[DataFrame] = Seq()
    for (group: ActivityMetricsGroup <- groups) {
      val dimensionMetadataMap = streamMetadata.get(group.getStream.getStreamId)
      val periodStoresMetadata = inputMetadata.getMetadata.get(group.getStream.getStreamId)

      if (shouldSkipGroup(group, dimensionMetadataMap)) { // create empty dataframe for skipped groups
        metrics :+= Seq.empty[String].toDF(DeriveAttrsUtils.getEntityIdColumnNameFromEntity(group.getEntity))
      } else {
        metrics :+= aggregateMetrics(group, evaluationDate, aggregatedPeriodStores, translator, periodStoresMetadata, dimensionMetadataMap)
      }
      detailsMap.put(group.getGroupId, setDetails(index))
      index += 1
    }
    outputMetadata.setMetadata(detailsMap)

    lattice.outputStr = serializeJson(outputMetadata)
    lattice.output = metrics.toList
  }

  private def aggregateMetrics(group: ActivityMetricsGroup,
                               evaluationDate: String,
                               aggregatedPeriodStores: Seq[DataFrame],
                               translator: TimeFilterTranslator,
                               periodStoresMetadata: ActivityStoreSparkIOMetadata.Details,
                               dimensionMetadataMap: util.Map[String, DimensionMetadata]): DataFrame = {
    // construct period map: period -> idx
    var offsetMap: Map[String, Int] = Map()
    for (idx <- 0 until periodStoresMetadata.getLabels.size) {
      offsetMap += (periodStoresMetadata.getLabels.get(idx) -> idx)
    }

    // divide dataframe by time filters defined in TimeRange SQL column
    // dataframe -> corresponding time range string
    val filteredByTime: Seq[(DataFrame, String)] = ActivityMetricsGroupUtils.toTimeFilters(group.getActivityTimeRange).toSeq
      .map(tf => {
        val periodStoreIndex: Int = periodStoresMetadata.getStartIdx + offsetMap(tf.getPeriod)
        separateByTimeFilter(aggregatedPeriodStores.get(periodStoreIndex), tf, translator, group)
      })

    // rollup by (entityId, rollupDimensions), pivot and rename pivoted attribute following attrName template
    val attrRolledUp: Seq[DataFrame] = filteredByTime.map(item => rollupAndCreateAttr(item._1, item._2, group, dimensionMetadataMap))

    // join dataframes from all time filters
    val entityIdColName = DeriveAttrsUtils.getEntityIdColumnNameFromEntity(group.getEntity)
    val joined: DataFrame = attrRolledUp.reduce((df1, df2) => {
      df1.join(df2, Seq(entityIdColName), "fullouter")
    })

    val missingEntitiesAppended: DataFrame = unifyBatchStore(joined, group.getEntity)

    // replace null values with defined method
    val replaceNull: DataFrame = group.getNullImputation match {
      case NULL => missingEntitiesAppended // no operation needed
      case ZERO => DeriveAttrsUtils.fillZero(missingEntitiesAppended, group.getJavaClass)
      case FALSE => DeriveAttrsUtils.fillFalse(missingEntitiesAppended, group.getJavaClass)
      case _ => throw new UnsupportedOperationException("Unknown null imputation method")
    }

    replaceNull
  }

  // return: (dataframe, TimeRange string used for attribute name template)
  def separateByTimeFilter(df: DataFrame, timeFilter: TimeFilter, translator: TimeFilterTranslator, group: ActivityMetricsGroup): (DataFrame, String) = {
    val periodIdColumnName: String = InterfaceName.PeriodId.name
    val bounds = translator.translateRange(timeFilter)

    val timeRangeStr: String = ActivityMetricsGroupUtils.timeFilterToTimeRangeTmpl(timeFilter)
    var inRange: DataFrame = {
      if (bounds == null) {
        df
      } else {
        df.filter(df(periodIdColumnName).between(bounds.getLeft, bounds.getRight))
      }
    }
    if (Option(group.getReducer).isDefined) {
      val reducer = Option(group.getReducer).get
      inRange = DeriveAttrsUtils.applyReducer(inRange, reducer)
    }
    (inRange, timeRangeStr)
  }

  def rollupAndCreateAttr(df: DataFrame, timeRangeName: String, group: ActivityMetricsGroup, dimensionMetadataMap: util.Map[String, DimensionMetadata]): DataFrame = {
    val deriver: StreamAttributeDeriver = group.getAggregation
    val entityIdColName: String = DeriveAttrsUtils.getEntityIdColumnNameFromEntity(group.getEntity)
    val rollupDimNames: Seq[String] = group.getRollupDimensions.split(",") :+ entityIdColName
    val pivotCols: Seq[String] = group.getRollupDimensions.split(",")
    var preprocessed: DataFrame = df

    if (group.getStream.getAggrEntities.contains(BusinessEntity.Contact.name) && BusinessEntity.Account.equals(group.getEntity)) {
      // for contact stream expecting account metrics, need to join with contact batch store for more accurate accountId
      preprocessed = preprocessContactStream(df)
    }

    val rolledUp: DataFrame = preprocessed.rollup(rollupDimNames.head, rollupDimNames.tail: _*).agg(DeriveAttrsUtils.getAggr(df, deriver))

    val excludeNull: DataFrame = rolledUp.na.drop // only required columns exist at this stage. drop all null

    def concatColumns: UserDefinedFunction = udf((row: Row) => row.mkString("_"))

    val pivoted: DataFrame = excludeNull.withColumn("combColumn", concatColumns(struct(pivotCols.map(col): _*))).groupBy(entityIdColName)
      .pivot("combColumn").agg(DeriveAttrsUtils.getAggr(excludeNull, deriver))

    var attrRenamed: DataFrame = pivoted.columns.foldLeft(pivoted) { (pivotedDF, colName) =>
      if (!colName.equals(entityIdColName)) {
        val attrName: String = constructAttrName(group.getGroupId, Seq(colName), timeRangeName)
        pivotedDF.withColumnRenamed(colName, attrName)
      } else {
        pivotedDF
      }
    }
    getRequiredAttrs(group, dimensionMetadataMap, timeRangeName)
      .filter(!attrRenamed.columns.contains(_))
      .foreach(attrName => attrRenamed = DeriveAttrsUtils.appendNullColumn(attrRenamed, attrName, group.getJavaClass))
    attrRenamed
  }

  def getRequiredAttrs(group: ActivityMetricsGroup, streamMetadata: util.Map[String, DimensionMetadata], timeRange: String): Seq[String] = {
    val groupDimensionNames: Array[String] = group.getRollupDimensions.split(",")
    val iterator: Iterator[String] = groupDimensionNames.iterator
    val dimName = iterator.next
    var rollupDimIds: Seq[String] = extractDimIds(streamMetadata.get(dimName), dimName)
    while (iterator.hasNext) {
      val dimName: String = iterator.next
      rollupDimIds = for (rollupDimIds <- rollupDimIds; newDimId <- extractDimIds(streamMetadata.get(dimName), dimName))
        yield rollupDimIds + "_" + newDimId
    }
    rollupDimIds.map(rollupDimIdTmpl => constructAttrName(group.getGroupId, Seq(rollupDimIdTmpl), timeRange))
  }

  def extractDimIds(metadata: DimensionMetadata, dimName: String): Seq[String] = {
    var ids: Seq[String] = Seq()
    for (dimVal: util.Map[String, AnyRef] <- metadata.getDimensionValues) {
      ids = ids :+ dimVal.get(dimName).toString
    }
    ids
  }

  private def constructAttrName(groupId: String, rollupDimIds: Seq[Object], timeRangeStr: String): String = {
    val map: util.Map[String, Object] = new util.HashMap[String, Object]()
    map.put(TMPLKEY_GROUPID, groupId)
    map.put(TMPLKEY_ROLLUP_DIM_IDs, seqAsJavaList(rollupDimIds))
    map.put(TMPLKEY_TIMERANGE, timeRangeStr)
    TemplateUtils.renderByMap(StringTemplateConstants.ACTIVITY_METRICS_GROUP_ATTRNAME, map).toLowerCase()
  }

  private def toPeriodStrategy(name: String, calendar: BusinessCalendar): PeriodStrategy = {
    new PeriodStrategy(calendar, Template.fromName(name))
  }

  private def getPeriodStrategies(groups: Seq[ActivityMetricsGroup], calendar: BusinessCalendar): util.List[PeriodStrategy] = {
    val periodSets: Seq[Set[String]] = groups.map((group: ActivityMetricsGroup) => group.getActivityTimeRange.getPeriods.toSet)
    val periodNames: Seq[String] = periodSets.reduce((masterSet, nextSet) => masterSet ++ nextSet).toSeq
    scala.collection.JavaConversions.seqAsJavaList(
      periodNames.map(name => toPeriodStrategy(name, calendar))
    )
  }

  def setDetails(index: Int): Details = {
    val details = new Details()
    details.setStartIdx(index)
    details
  }

  def unifyBatchStore(df: DataFrame, entity: BusinessEntity): DataFrame = {
    // remove entities not in batch store, append entities missing from batch store
    val entityIdCol: String = DeriveAttrsUtils.getEntityIdColumnNameFromEntity(entity)
    val batchStore =  entity match {
      case BusinessEntity.Account => accountBatchStoreTable
      case BusinessEntity.Contact => contactBatchStoreTable
      case _ => throw new UnsupportedOperationException(s"entity $entity is not supported for activity store")
    }
    df.join(batchStore.select(entityIdCol), Seq(entityIdCol), "right")
  }

  def shouldSkipGroup(group: ActivityMetricsGroup, dimensionMetadataMap: util.Map[String, DimensionMetadata]): Boolean = {
    rollupDimensionEmpty(group, dimensionMetadataMap) || missingBatchStore(group.getEntity)
  }

  def rollupDimensionEmpty(group: ActivityMetricsGroup, dimensionMetadataMap: util.Map[String, DimensionMetadata]): Boolean = {
    group.getRollupDimensions.split(",").exists(name => dimensionMetadataMap.get(name).getCardinality <= 0)
  }

  def missingBatchStore(entity: BusinessEntity): Boolean = {
    entity match {
      case BusinessEntity.Account => !hasAccountBatchStore
      case BusinessEntity.Contact => !hasContactBatchStore
      case _ => throw new UnsupportedOperationException(s"$entity should not have batch store check")
    }
  }

  def preprocessContactStream(df: DataFrame): DataFrame = {
    val accountIdCol = InterfaceName.AccountId.name
    val contactIdCol = InterfaceName.ContactId.name
    df.drop(accountIdCol).join(contactBatchStoreTable.select(accountIdCol, contactIdCol), Seq(contactIdCol), "inner")
  }
}
