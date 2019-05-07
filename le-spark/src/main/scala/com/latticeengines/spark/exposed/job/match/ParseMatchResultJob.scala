package com.latticeengines.spark.exposed.job.`match`

import com.latticeengines.domain.exposed.datacloud.`match`.MatchConstants
import com.latticeengines.domain.exposed.datacloud.`match`.MatchConstants.{INT_LDC_DEDUPE_ID, INT_LDC_LID, INT_LDC_REMOVED}
import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.serviceflows.core.spark.ParseMatchResultJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._

class ParseMatchResultJob extends AbstractSparkJob[ParseMatchResultJobConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[ParseMatchResultJobConfig]): Unit = {
    // read input
    val matchResult: DataFrame = lattice.input.head
    val sourceTable: DataFrame = if (lattice.input.size == 2) lattice.input(1) else null

    val config: ParseMatchResultJobConfig = lattice.config
    val srcCols: List[String] = if (config.sourceColumns == null) Nil else config.sourceColumns.asScala.toList

    // calculation
    var result = resolveConflictingFields(matchResult)
    if (config.excludeDataCloudAttrs) {
      result = excludeDCAttrs(result, srcCols, config.keepLid)
    }

    if (sourceTable != null) {
      val internalId: Option[String] =
        if (result.columns.contains(InterfaceName.InternalId.name)) Some(InterfaceName.InternalId.name) else None
      val matchGroupId: Option[String] =
        if (StringUtils.isNotBlank(config.matchGroupId)) Some(config.matchGroupId) else None
      val joinKey = if (config.joinInternalId) {
        internalId.orElse(matchGroupId).orElse(null).get
      } else {
        matchGroupId.orElse(internalId).orElse(null).get
      }

      if (joinKey != null) {
        result = joinSourceTable(result, sourceTable, joinKey)
      }
    }

    // finish
    lattice.output = result::Nil
  }

  def resolveConflictingFields(result: DataFrame): DataFrame = {
    val resultCols = result.columns.toSet
    val prefix = MatchConstants.SOURCE_PREFIX
    // if we have both "f1" and "Source_f1" in schema remove "f1"
    // then rename all "Source_x" to "x"
    val withPrefix = resultCols.filter(field => field.startsWith(prefix))
    val withOutPrefix = resultCols.filterNot(field => field.startsWith(prefix))
    val conflictingFields = withOutPrefix.intersect(withPrefix.map(field => field.substring(prefix.length))).toList
    val dropped = result.drop(conflictingFields:_*)
    removePrefix(dropped)
  }

  def removePrefix(df: DataFrame): DataFrame = {
    val prefix = MatchConstants.SOURCE_PREFIX
    val newNames = df.columns map { field =>
      if (field.startsWith(prefix)) {
        field.substring(prefix.length)
      } else {
        field
      }
    }
    df.toDF(newNames: _*)
  }

  def excludeDCAttrs(result: DataFrame, srcAttrs: List[String], keepLid: Boolean): DataFrame = {
    // only keep src attrs + some internal dc attrs
    val resultAttrs = result.columns
    val hasDedupId = resultAttrs.contains(MatchConstants.INT_LDC_DEDUPE_ID)
    val hasInternalId = resultAttrs.contains(InterfaceName.InternalId.name)
    var retainAttrs = if (keepLid) InterfaceName.LatticeAccountId.name::srcAttrs else srcAttrs
    retainAttrs = if (hasDedupId) INT_LDC_LID::INT_LDC_DEDUPE_ID::INT_LDC_REMOVED::retainAttrs else retainAttrs
    val dropAttrs = resultAttrs.diff(retainAttrs)
    result.drop(dropAttrs:_*)
  }

  def joinSourceTable(matchResult: DataFrame, sourceTable: DataFrame, joinKey: String): DataFrame = {
    val retainAttrs = joinKey::matchResult.columns.diff(sourceTable.columns).toList
    val dropAttrs = matchResult.columns.diff(retainAttrs)
    val reducedResult = matchResult.drop(dropAttrs:_*).persist(StorageLevel.DISK_ONLY)
    val count = reducedResult.count()
    val partitions = math.max(count / 4000, 200).toInt
    reducedResult.join(sourceTable.repartition(partitions, col(joinKey)), Seq(joinKey))
  }

}
