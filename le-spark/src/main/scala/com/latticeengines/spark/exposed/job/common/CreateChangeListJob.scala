package com.latticeengines.spark.exposed.job.common

import com.latticeengines.domain.exposed.spark.common.ChangeListConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.{ChangeListUtils, MergeUtils}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable.{Seq => MSeq}

class CreateChangeListJob extends AbstractSparkJob[ChangeListConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[ChangeListConfig]): Unit = {
    val config: ChangeListConfig = lattice.config
    val toTable = lattice.input.head
    val fromTable: DataFrame = if (lattice.input.size == 2) lattice.input(1) else null
    val joinKey = config.getJoinKey
    val exclusionCols = config.getExclusionColumns.asScala.toSet
    val params = new Params
    params.fromTableColPos = if (fromTable != null) fromTable.columns.zipWithIndex.toMap else null
    params.toTableColPos = toTable.columns.zipWithIndex.toMap
    params.fromTableColType = if (fromTable != null) fromTable.dtypes.toMap else null
    params.toTableColType = toTable.dtypes.toMap
    if (config.getJoinType != null) {
      params.joinType = config.getJoinType
    }

    val result =
      if (fromTable == null) {
        createForNewTable(spark, toTable, exclusionCols, joinKey, params)
      } else {
        createChangeList(spark, toTable, fromTable, exclusionCols, joinKey, params)
      }

    lattice.output = result :: Nil
  }

  private def createForNewTable(spark: SparkSession, toTable: DataFrame, exclusionCols: Set[String], joinKey: String, params: Params): DataFrame = {
    val cols = toTable.columns.filter(c => !exclusionCols.contains(c))
    val bToColPos = spark.sparkContext.broadcast(params.toTableColPos)
    val bToTableColType = spark.sparkContext.broadcast(params.toTableColType)
    val changeList = toTable.flatMap(row => {
      val toColPos = bToColPos.value
      val toTableColType = bToTableColType.value
      var rows: MSeq[Option[Row]] =  cols map (colName => {
        ChangeListUtils.buildNewRowColumnList(row, colName, joinKey, toColPos, toColPos, toTableColType)
      })
      val rowList = ChangeListUtils.buildNewRowList(row, joinKey, toColPos)
      rows = rowList +: rows
      rows.flatten
    })(RowEncoder(ChangeListUtils.changeListSchema))
    changeList
  }

  private def createChangeList(spark: SparkSession, toTable: DataFrame, fromTable: DataFrame,
      exclusionCols: Set[String], joinKey: String, params: Params): DataFrame = {

    val commonColNames = fromTable.columns.intersect(toTable.columns).diff(Seq(joinKey).union(exclusionCols.toSeq))
    val deletedColNames = fromTable.columns.diff(toTable.columns.union(exclusionCols.toSeq))
    val newColNames = toTable.columns.diff(fromTable.columns.union(exclusionCols.toSeq))

    val bDeleteColPos = spark.sparkContext.broadcast(params.fromTableColPos)
    val bDeleteColType = spark.sparkContext.broadcast(params.fromTableColType)
    val deletedColList =  fromTable.flatMap(row => {
      val rows: Seq[Option[Row]] = deletedColNames.par.map(colName => {
        ChangeListUtils.buildDeletedRowColumnList(row, colName, joinKey, bDeleteColPos.value, bDeleteColType.value)
      }).seq
      rows.flatten
    })(RowEncoder(ChangeListUtils.changeListSchema))
      .union(spark.createDataFrame(
        spark.sparkContext.parallelize(ChangeListUtils.buildDeletedColumnList(deletedColNames.toList)),
        ChangeListUtils.changeListSchema))

    val bNewColPos = spark.sparkContext.broadcast(params.toTableColPos)
    val bNewColType = spark.sparkContext.broadcast(params.toTableColType)
    val newColList = toTable.flatMap(row => {
      val rows: Seq[Option[Row]] = newColNames.par.map(colName => {
        ChangeListUtils.buildNewColumnList(row, colName, joinKey, bNewColPos.value, bNewColType.value)
      }).seq
      rows.flatten
    })(RowEncoder(ChangeListUtils.changeListSchema))

    val slimFromTable = fromTable.drop(deletedColNames:_*).repartition(col(joinKey))
    val slimToTable = toTable.drop(newColNames:_*).repartition(col(joinKey))
    val joined = MergeUtils.joinWithMarkers(slimFromTable, slimToTable, Seq(joinKey), joinType = params.joinType)
    val (fromColPos, toColPos) = MergeUtils.getColPosOnBothSides(joined)
    val (fromMarker, toMarker) = MergeUtils.joinMarkers()

    val bFromColPos = spark.sparkContext.broadcast(fromColPos)
    val bFromTableColType = spark.sparkContext.broadcast(params.fromTableColType)
    val bToColPos = spark.sparkContext.broadcast(toColPos)
    val bToTableColType = spark.sparkContext.broadcast(params.toTableColType)

    val changedList = joined.flatMap(row => {
      val isDeletedRow = row.getAs(fromMarker) != null && row.getAs(toMarker) == null
      val isNewRow = row.getAs(fromMarker) == null && row.getAs(toMarker) != null

      val fromColPos = bFromColPos.value
      val fromTableColType = bFromTableColType.value
      val toColPos = bToColPos.value
      val toTableColType = bToTableColType.value

      var rows: MSeq[Option[Row]] = MSeq()
      if (isDeletedRow) {
        rows = rows :+ ChangeListUtils.buildDeletedRowList(row, joinKey, fromColPos)
      } else if (isNewRow) {
        rows = rows :+ ChangeListUtils.buildNewRowList(row, joinKey, fromColPos)
      }
      if (isDeletedRow) {
        val newRows = commonColNames map (colName => {
          ChangeListUtils.buildDeletedRowColumnList(row, colName, joinKey, fromColPos, fromTableColType)
        })
        rows = rows ++ newRows
      } else if (isNewRow) {
        val newRows = commonColNames map (colName => {
          ChangeListUtils.buildNewRowColumnList(row, colName, joinKey, fromColPos, toColPos, toTableColType)
        })
        rows = rows ++ newRows
      } else {
        val newRows = commonColNames map (colName => {
          ChangeListUtils.buildCommonRowColumnList(row, colName, joinKey, fromColPos, toColPos, toTableColType)
        })
        rows = rows ++ newRows
      }
      rows.flatten
    })(RowEncoder(ChangeListUtils.changeListSchema))

    changedList.union(deletedColList).union(newColList)
  }
}

class Params extends Serializable {
  var fromTableColPos: Map[String, Int] = _
  var toTableColPos: Map[String, Int] = _
  var fromTableColType: Map[String, String] = _
  var toTableColType: Map[String, String] = _
  var joinType: String = "outer"
}

