package com.latticeengines.spark.exposed.job.common

import com.latticeengines.domain.exposed.spark.common.CopyConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.MergeUtils
import org.apache.commons.collections4.MapUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

/**
  * Copy a single dataset, can perform simple column selection and rename
  * Operation sequence: select -> drop -> rename -> add create/update timestamps
  */
class CopyJob extends AbstractSparkJob[CopyConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[CopyConfig]): Unit = {
    val config: CopyConfig = lattice.config
    val inputs: List[DataFrame] = lattice.input
    val dfs = inputs map {df => processDf(df, config)}
    val concatenated = dfs reduce {(d1, d2) => MergeUtils.concat2(d1, d2)}
    lattice.output = concatenated :: Nil
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
        val (colsToSelectOrDrop, selectMode) = getColsToSelectOrDrop(input.columns.toList, colsToSelect, colsToDrop)
        if (selectMode) {
          input.select(colsToSelectOrDrop map col: _*)
        } else {
          input.drop(colsToSelectOrDrop: _*)
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

}
