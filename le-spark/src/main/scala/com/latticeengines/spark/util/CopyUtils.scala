package com.latticeengines.spark.util

import com.latticeengines.domain.exposed.spark.common.CopyConfig
import org.apache.commons.collections4.MapUtils
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.JavaConverters._

private[spark] object CopyUtils {

  def copy(config: CopyConfig, inputs: List[DataFrame]): DataFrame = {
    val dfs = inputs map {df => processDf(df, config)}
    dfs reduce {(d1, d2) => MergeUtils.concat2(d1, d2)}
  }

  private def processDf(input: DataFrame, config: CopyConfig): DataFrame = {
    val colsToSelect: Option[List[String]] =
      if (config.getSelectAttrs == null)
        None
      else
        Some(config.getSelectAttrs.asScala.toList)

    val colsToDrop: Option[List[String]] =
      if (config.getDropAttrs == null)
        None
      else
        Some(config.getDropAttrs.asScala.toList)

    val filtered = (colsToSelect, colsToDrop) match {
      case (None, None) => input
      case _ =>
        val (colsToSelectOrDrop, isSelectMode) = getColsToSelectOrDrop(input.columns.toList, colsToSelect, colsToDrop)
        if (isSelectMode) {
          selectCols(input, colsToSelectOrDrop)
        } else {
          dropCols(input, colsToSelectOrDrop)
        }
    }

    val renamed =
      if (MapUtils.isEmpty(config.getRenameAttrs)) {
        filtered
      } else {
        val attrsToRename: Map[String, String] = config.getRenameAttrs.asScala.toMap
          .filterKeys(filtered.columns.contains(_))
        val newAttrs = filtered.columns.map(c => attrsToRename.getOrElse(c, c))
        filtered.toDF(newAttrs: _*)
      }

    renamed
  }

  // second return toggles select vs drop
  private def getColsToSelectOrDrop(colsInDf: List[String], colsToSelect: Option[List[String]],
                                    colsToDrop: Option[List[String]]): (List[String], Boolean) = {
    val selected = colsToSelect match {
      case None => colsInDf
      case Some(lst) => colsInDf.intersect(lst)
    }

    val dropped = colsToDrop match {
      case None => selected
      case Some(lst) => selected.diff(lst)
    }

    if (dropped.length < colsInDf.length / 3) {
      (dropped, true)
    } else {
      (colsInDf.diff(dropped), false)
    }

  }

  private def selectCols(input: DataFrame, colsToSelect: Seq[String]): DataFrame = {
    val colSet = colsToSelect.toSet
    val fieldIdxPairs = input.schema.zipWithIndex filter { t => colSet.contains(t._1.name) }
    val outputSchema = StructType(fieldIdxPairs map { t => t._1 })
    val selectedIdx: Set[Int] = (fieldIdxPairs map { t => t._2 }).toSet
    input.map(row => {
      val vals = row.toSeq.zipWithIndex.filter(t => selectedIdx.contains(t._2)).map(t => t._1)
      Row.fromSeq(vals)
    })(RowEncoder(outputSchema))
  }

  private def dropCols(input: DataFrame, colsToDrop: Seq[String]): DataFrame = {
    val colSet = colsToDrop.toSet
    val remainFields = input.schema.filter(f => !colSet.contains(f.name))
    val outputSchema = StructType(remainFields)
    val dropIdx: Set[Int] = input.columns.zipWithIndex.filter(t => colSet.contains(t._1)).map(t => t._2).toSet
    input.map(row => {
      val vals = row.toSeq.zipWithIndex.filter(t => !dropIdx.contains(t._2)).map(t => t._1)
      Row.fromSeq(vals)
    })(RowEncoder(outputSchema))
  }

}
