package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.spark.cdl.MergeLatticeAccountConfig
import com.latticeengines.domain.exposed.spark.common.{ChangeListConstants, CopyConfig}
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.CopyUtils
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

/**
  * MergeLatticeAccount is different from normal Upsert or Merge job in that
  * 1. The LatticeAccount table is extremely wide
  * 2. The new table has all the columns, due to the fact that match api is not a columnar store
  *
  * Therefore, merging LatticeAccount can be simply take new table as a whole,
  * then union with the missing rows from old table and align the schema to the new table
  */
class MergeLatticeAccount extends AbstractSparkJob[MergeLatticeAccountConfig] {

  private val AccountId = InterfaceName.AccountId.name
  private val CDLCreatedTime = InterfaceName.CDLCreatedTime.name

  override def runJob(spark: SparkSession, lattice: LatticeContext[MergeLatticeAccountConfig]): Unit = {
    val oldTbl = lattice.input.head
    val newTbl = lattice.input(1)
    val config = lattice.config

    val result = if (ChangeListConstants.VerticalMode.equals(config.getMergeMode)) {
      mergeVertically(newTbl, oldTbl)
    } else {
      mergeHorizontally(newTbl, oldTbl)
    }

    lattice.output = List(result)
  }

  /**
    * In Vertical mode:
    * Divide old table in 4 parts:
    * ---------
    *      |
    *   A  | D
    *      |
    * ---------
    *   B  | C
    * ---------
    *
    * Use new table to replace "D"
    *
    */
  private def mergeVertically(newTbl: DataFrame, oldTbl: DataFrame): DataFrame = {
    val newMarker = "__new__"
    val newIds = newTbl.select(AccountId).withColumn(newMarker, lit(true))
    val joined = oldTbl.join(newIds, Seq(AccountId), joinType = "left")
    val partBC = joined.filter(col(newMarker).isNull).drop(newMarker)
    val partAD = joined.filter(col(newMarker) === true).drop(newMarker)

    val newTblWithTs = CopyUtils.fillTimestamps(newTbl).drop(CDLCreatedTime)
    val newCols = newTblWithTs.columns // columns for part C and D
    val oldCols = oldTbl.columns.diff(newCols).union(Seq(AccountId)) // columns for part A and B
    val partA = partAD.select(oldCols map col:_*)
    val newAD = partA.join(newTblWithTs, Seq(AccountId), joinType = "left")

    val newBC = alignOldTblSchema(newAD, partBC)
    newAD unionByName newBC
  }

  /**
    * In Horizontal mode:
    * The new table has all the columns
    * take new table as a whole,
    * then union with the missing rows from old table and align the schema to the new table
    */
  private def mergeHorizontally(newTbl: DataFrame, oldTbl: DataFrame): DataFrame = {
    val newIds = newTbl.select(AccountId)
    val rollover = oldTbl.join(newIds, Seq(AccountId), joinType = "left_anti")
    val newTblWithTs = CopyUtils.fillTimestamps(newTbl)
    val filtered = filterOldTbl(newTblWithTs, rollover)
    newTblWithTs unionByName filtered
  }

  private def filterOldTbl(newTbl: DataFrame, oldTbl: DataFrame): DataFrame = {
    val filteredCols: Seq[String] = oldTbl.columns.intersect(newTbl.columns)
    val filtered = if (filteredCols.length < oldTbl.columns.length) {
      val copyConfig = new CopyConfig
      copyConfig.setSelectAttrs(filteredCols.asJava)
      CopyUtils.copy(copyConfig, Seq(oldTbl))
    } else {
      oldTbl
    }
    alignOldTblSchema(newTbl, filtered)
  }

  private def alignOldTblSchema(newTbl: DataFrame, oldTbl: DataFrame): DataFrame = {
    val newCols = newTbl.columns.diff(oldTbl.columns)
    if (newCols.isEmpty) {
      oldTbl
    } else {
      val fields = newTbl.schema.fields.filter(f => newCols.contains(f.name))
      fields.foldLeft(oldTbl)((df, f) => df.withColumn(f.name, lit(null).cast(f.dataType)))
    }
  }

}
