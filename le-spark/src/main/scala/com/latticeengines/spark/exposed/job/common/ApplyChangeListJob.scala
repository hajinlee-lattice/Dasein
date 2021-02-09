package com.latticeengines.spark.exposed.job.common

import com.latticeengines.domain.exposed.spark.common.ApplyChangeListConfig
import com.latticeengines.domain.exposed.spark.common.ChangeListConstants._
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.{ChangeListUtils, MergeUtils}
import org.apache.spark.sql.functions.{col, first}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._

class ApplyChangeListJob extends AbstractSparkJob[ApplyChangeListConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[ApplyChangeListConfig]): Unit = {
    val config: ApplyChangeListConfig = lattice.config
    val sourceOpt: Option[DataFrame] = if (config.isHasSourceTbl) Some(lattice.input.head) else None
    val changeListInputs = if (config.isHasSourceTbl) lattice.input.tail else lattice.input
    val joinKey = config.getJoinKey
    val includeAttrs: Seq[String] = if (config.getIncludeAttrs == null) Seq() else config.getIncludeAttrs.toSeq

    val changeList = changeListInputs.reduce(_ union _)
    val allUpdates = changeList
      .filter(col(RowId).isNotNull && col(ColumnId).isNotNull && (col(Deleted).isNull || col(Deleted) === false))
    val validUpdates = (if (includeAttrs.isEmpty) {
      allUpdates
    } else {
      ChangeListUtils.filterByCols(spark, allUpdates, includeAttrs)
    }).checkpoint()

    val result = sourceOpt match {
      case None => pivotChangeList(spark, validUpdates, joinKey)
      case Some(source) =>
        val deletedRowIds = changeList
          .filter(col(RowId).isNotNull && col(ColumnId).isNull && col(Deleted) === true)
          .select(col(RowId).alias(joinKey)).distinct.persist(StorageLevel.DISK_ONLY)
        val deletedColumnIds = changeList
          .filter(col(RowId).isNull && col(ColumnId).isNotNull && col(Deleted) === true)
          .select(col(ColumnId)).distinct
        val deletedColumnNames = deletedColumnIds.collect.map(r => r.getString(0)).toSeq
        mergeWithExisting(spark, source, deletedRowIds, deletedColumnNames, validUpdates, joinKey)
    }
    lattice.output = result :: Nil
  }

  def mergeWithExisting(spark: SparkSession, source: DataFrame, deletedRowIds: DataFrame, deletedColumnNames: Seq[String],
    updatedChangeList: DataFrame, joinKey: String): DataFrame = {
    var result = source

    if (deletedColumnNames.nonEmpty) {
      result = result.drop(deletedColumnNames: _*)
    }

    if (deletedRowIds.count > 0) {
      result = MergeUtils.joinWithMarkers(result, deletedRowIds, Seq(joinKey), "left")
      val (fromMarker, toMarker) = MergeUtils.joinMarkers()
      result = result.filter(col(toMarker).isNull)
      result = result.drop(fromMarker, toMarker)
    }

    val updatedRowCount = updatedChangeList.count()
    if (updatedRowCount > 0) {
      val pivotedChangeList = pivotChangeList(spark, updatedChangeList, joinKey).checkpoint()
      result = result.repartition(col(joinKey))
      result = MergeUtils.merge2(result, pivotedChangeList, Seq(joinKey), Set(), overwriteByNull = false)
    }
    result
  }

  def pivotChangeList(spark: SparkSession, changeList: DataFrame, joinKey: String): DataFrame = {
    val dataTypes = changeList.select(DataType).distinct.collect().map(r => r.getString(0))
    dataTypes.foldLeft(null: DataFrame)((result, dType) => {
      val changeOfType = changeList //
        .filter(col(DataType) === dType).select(RowId, ColumnId, s"$From$dType", s"$To$dType")
      val cols = changeOfType.select(ColumnId).distinct.collect.map(r => r.getString(0)).toSeq
      val pivoted = pivotTypeChangeList(spark, changeOfType, cols, dType)
      if (result == null) {
        pivoted.persist(StorageLevel.DISK_ONLY)
      } else {
        MergeUtils.merge2(result, pivoted, Seq(RowId), Set(), overwriteByNull = false).persist(StorageLevel.DISK_ONLY)
      }
    }).withColumnRenamed(RowId, joinKey)
  }

  def pivotTypeChangeList(spark: SparkSession, changeList: DataFrame, cols: Seq[String], dataType: String): DataFrame = {
    if (cols.length <= 500) {
      ChangeListUtils.filterByCols(spark, changeList, cols)
        .groupBy(RowId).pivot(ColumnId).agg(first(columnName = s"$To$dataType"))
    } else {
      val (p1, p2) = cols.splitAt(500)
      val pivot1 = pivotTypeChangeList(spark, changeList, p1, dataType)
      val pivot2 = pivotTypeChangeList(spark, changeList, p2, dataType)
      MergeUtils.merge2(pivot2, pivot1, Seq(RowId), Set(), overwriteByNull = false)
    }
  }

}

