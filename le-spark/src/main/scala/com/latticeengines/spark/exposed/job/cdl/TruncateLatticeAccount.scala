package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.spark.cdl.TruncateLatticeAccountConfig
import com.latticeengines.domain.exposed.spark.common.ChangeListConstants.RowId
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.ChangeListUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._

/**
  * This job to to remove rows and columns for old LatticeAccount table
  * It also generates the corresponding part of the ChangeList
  */
class TruncateLatticeAccount extends AbstractSparkJob[TruncateLatticeAccountConfig] {

  private val AccountId = InterfaceName.AccountId.name
  private val delete_marker = "__delete_marker__"

  override def runJob(spark: SparkSession, lattice: LatticeContext[TruncateLatticeAccountConfig]): Unit = {
    val baseTbl = lattice.input.head
    val deleteLstOpt = lattice.input.lift(1)
    val config = lattice.config
    val attrs2Remove: Seq[String] = if (config.getRemoveAttrs == null) {
      Seq()
    } else {
      config.getRemoveAttrs.asScala.intersect(baseTbl.columns)
    }

    val (trimTbl: DataFrame, removeAttrsChangeList: Option[DataFrame]) = {
      val ignoreChangeList: Boolean = if (config.getIgnoreRemoveAttrsChangeList == null) false else config.getIgnoreRemoveAttrsChangeList
      removeAttrs(spark, baseTbl, attrs2Remove, ignoreChangeList)
    }

    val (deletedTbl: DataFrame, deletedRowsChangeList: Option[DataFrame]) = deleteLstOpt match {
      case Some(df) =>
        val deleteLst = df.select(RowId).withColumnRenamed(RowId, AccountId).withColumn(delete_marker, lit(true))
        val ignoreAttrs: Seq[String] = if (config.getIgnoreAttrs == null) Seq() else config.getIgnoreAttrs.asScala
        deleteRows(spark, trimTbl, deleteLst, ignoreAttrs)
      case None => (trimTbl, None)
    }

    val changeList = (removeAttrsChangeList, deletedRowsChangeList) match {
      case (Some(df1), Some(df2)) => df1 union df2
      case (Some(df1), None) => df1
      case (None, Some(df2)) => df2
      case (None, None) =>
        val rdd: RDD[Row] = spark.sparkContext.parallelize(Seq())
        spark.createDataFrame(rdd, ChangeListUtils.changeListSchema)
    }

    lattice.output = List(deletedTbl, changeList)
  }

  private def removeAttrs(spark: SparkSession, baseTbl: DataFrame, attrs2Remove: Seq[String], ignoreChangeList: Boolean): (DataFrame, Option[DataFrame]) = {
    if (attrs2Remove.isEmpty) {
      (baseTbl, None)
    } else {
      val removed = baseTbl.drop(attrs2Remove:_*)
      if (ignoreChangeList) {
        (removed, None)
      } else {
        val rdd = spark.sparkContext.parallelize(ChangeListUtils.buildDeletedColumnList(attrs2Remove))
        val df = spark.createDataFrame(rdd, ChangeListUtils.changeListSchema)
        (removed, Some(df))
      }
    }
  }

  private def deleteRows(spark: SparkSession, baseTbl: DataFrame, deleteLst: DataFrame, ignoreAttrs: Seq[String]): (DataFrame, Option[DataFrame]) = {
    val join = baseTbl.join(deleteLst, Seq(AccountId), joinType = "left").persist(StorageLevel.DISK_ONLY)
    val remain = join.filter(col(delete_marker).isNull).drop(delete_marker)
    val deleted = join.filter(col(delete_marker).isNotNull)
      .drop((ignoreAttrs :+ delete_marker).diff(List(AccountId)): _*)
    val changeListAttrs = deleted.columns.diff(List(AccountId))
    val colType = spark.sparkContext.broadcast(deleted.dtypes.view.toMap)
    val colPos = spark.sparkContext.broadcast(deleted.columns.zipWithIndex.toMap)
    val changeList = deleted.flatMap(row => {
      val colId = InterfaceName.AccountId.name // serialize class variable reference
      val rows: Seq[Option[Row]] = changeListAttrs.par.map(colName => {
        ChangeListUtils.buildDeletedRowColumnList(row, colName, colId, colPos.value, colType.value)
      }).seq :+ ChangeListUtils.buildDeletedRowList(row, colId, colPos.value)
      rows.flatten
    })(RowEncoder(ChangeListUtils.changeListSchema))
    (remain, Some(changeList))
  }

}


