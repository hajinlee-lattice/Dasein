package com.latticeengines.spark.util

import com.latticeengines.domain.exposed.spark.common.CopyConfig
import org.apache.commons.collections4.MapUtils
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

private[spark] object CopyUtils {

  def copy(spark: SparkSession, config: CopyConfig, inputs: List[DataFrame]): DataFrame = {
    val dfs = inputs map { df => processDf(spark, df, config) }
    dfs reduce { (d1, d2) => MergeUtils.concat2(d1, d2) }
  }

  private def processDf(spark: SparkSession, input: DataFrame, config: CopyConfig): DataFrame = {
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
          selectCols(spark, input, colsToSelectOrDrop)
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

  private def selectCols(spark: SparkSession, input: DataFrame, colsToSelect: Seq[String]): DataFrame = {
    val colSet = colsToSelect.toSet
    val fieldIdxPairs = input.schema.zipWithIndex filter { t => colSet.contains(t._1.name) }
    val outputSchema = StructType(fieldIdxPairs map { t => t._1 })
    val selectedIdx: Seq[Int] = fieldIdxPairs map { t => t._2 }
    spark.createDataFrame(input.rdd.map(row => {
      val seq: ArrayBuffer[Any] = ArrayBuffer()
      for (index <- selectedIdx) {
        seq += row.get(index)
      }
      Row.fromSeq(seq)
    }), outputSchema)
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
