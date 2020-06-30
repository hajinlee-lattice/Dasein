package com.latticeengines.spark.util

import com.latticeengines.domain.exposed.spark.common.ChangeListConstants
import com.latticeengines.domain.exposed.spark.common.ChangeListConstants._
import org.apache.spark.sql.types.{DataType => _, _}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.{Seq => MSeq}

private[spark] object ChangeListUtils {

  private[spark] val changeListSchema = StructType(List(
    StructField(ChangeListConstants.RowId, StringType, nullable = true),
    StructField(ChangeListConstants.ColumnId, StringType, nullable = true),
    StructField(ChangeListConstants.DataType, StringType, nullable = true),
    StructField(ChangeListConstants.Deleted, BooleanType, nullable = true),
    StructField(ChangeListConstants.FromString, StringType, nullable = true),
    StructField(ChangeListConstants.ToString, StringType, nullable = true),
    StructField(ChangeListConstants.FromBoolean, BooleanType, nullable = true),
    StructField(ChangeListConstants.ToBoolean, BooleanType, nullable = true),
    StructField(ChangeListConstants.FromInteger, IntegerType, nullable = true),
    StructField(ChangeListConstants.ToInteger, IntegerType, nullable = true),
    StructField(ChangeListConstants.FromLong, LongType, nullable = true),
    StructField(ChangeListConstants.ToLong, LongType, nullable = true),
    StructField(ChangeListConstants.FromFloat, FloatType, nullable = true),
    StructField(ChangeListConstants.ToFloat, FloatType, nullable = true),
    StructField(ChangeListConstants.FromDouble, DoubleType, nullable = true),
    StructField(ChangeListConstants.ToDouble, DoubleType, nullable = true)
  ))

  private val outputColPos = changeListSchema.names.zipWithIndex.toMap

  def getToValue(row: Row): Any = {
      row.getAs[String](DataType) match {
        case "String" => row.getAs[String](ToString)
        case "Boolean" => row.getAs[Boolean](ToBoolean)
        case "Integer" => row.getAs[Int](ToInteger)
        case "Long" => row.getAs[Long](ToLong)
        case "Float" => row.getAs[Float](ToFloat)
        case "Double" => row.getAs[Double](ToDouble)
      }
  }

  def getFromValue(row: Row): Any = {
    row.getAs[String](DataType) match {
      case "String" => row.getAs[String](FromString)
      case "Boolean" => row.getAs[Boolean](FromBoolean)
      case "Integer" => row.getAs[Int](FromInteger)
      case "Long" => row.getAs[Long](FromLong)
      case "Float" => row.getAs[Float](FromFloat)
      case "Double" => row.getAs[Double](FromDouble)
    }
  }

  def buildNewRowList(row: Row, joinKey: String, toTableColPos: Map[String, Int]) : Option[Row] = {
    val rowId = row.get(toTableColPos(joinKey))
    val record = getRowTemplate(rowId, null, null, None)
    Some(Row.fromSeq(record))
  }

  def buildDeletedRowList(row: Row, joinKey: String, fromColPos: Map[String, Int]) : Option[Row] = {
    val rowId = row.get(fromColPos(joinKey))
    val record = getRowTemplate(rowId, null, null, Some(true))
    Some(Row.fromSeq(record))
  }

  def buildDeletedColumnList(columns: Seq[String]) : Seq[Row] = {
    columns.par.map( col => {
      val record = getRowTemplate(null, col, null, Some(true))
      Row.fromSeq(record)
    }).seq
  }

  def buildDeletedRowColumnList(row: Row, colName: String, joinKey: String, fromColPos: Map[String, Int],
                                fromTableColType: Map[String, String]) : Option[Row] = {
    val rowId = row.get(fromColPos(joinKey))
    val value = row.get(fromColPos(colName))
    if (value == null) {
      return None
    }
    val colType = fromTableColType(colName).dropRight(4)
    val record = getRowTemplate(rowId, colName, colType, Some(true))
    val fromCol = From + colType
    record(outputColPos(fromCol)) = value
    Some(Row.fromSeq(record))
  }

  def buildNewRowColumnList(row: Row, colName: String, joinKey: String,
                            fromColPos: Map[String, Int], toColPos: Map[String, Int],
                            toTableColType: Map[String, String]) : Option[Row] = {

    val rowId = row.get(fromColPos(joinKey))
    val value = row.get(toColPos(colName))
    if (value == null) {
      None
    } else {
      val colType = toTableColType(colName).dropRight(4)
      val record = getRowTemplate(rowId, colName, colType, None)
      val toCol = To + colType
      record(outputColPos(toCol)) = value
      Some(Row.fromSeq(record))
    }
  }

  // from new columns, no need to check old value
  def buildNewColumnList(row: Row, colName: String, joinKey: String, colPos: Map[String, Int],
                         colTypeMap: Map[String, String]) : Option[Row] = {
    val rowId = row.get(colPos(joinKey))
    val value = row.get(colPos(colName))
    if (value == null) {
      None
    } else {
      val colType = colTypeMap(colName).dropRight(4)
      val record = getRowTemplate(rowId, colName, colType, None)
      val toCol = To + colType
      record(outputColPos(toCol)) = value
      Some(Row.fromSeq(record))
    }
  }

  def buildCommonRowColumnList(row: Row, colName: String, joinKey: String,
                               fromColPos: Map[String, Int], toColPos: Map[String, Int],
                               toTableColType: Map[String, String]) : Option[Row] = {
    val fromVal = row.get(fromColPos(colName))
    val toVal = row.get(toColPos(colName))
    if (fromVal == null && toVal == null) {
      return None
    }
    if (fromVal != null && toVal != null && fromVal == toVal) {
      return None
    }
    val rowId = row.get(fromColPos(joinKey))
    val colType = toTableColType(colName).dropRight(4)
    val record = getRowTemplate(rowId, colName, colType, None)
    val fromCol = From + colType
    record(outputColPos(fromCol)) = fromVal
    val toCol = To + colType
    record(outputColPos(toCol)) = toVal

    Some(Row.fromSeq(record))
  }

  def getRowTemplate(rowId: Any, columnId: Any, colType: Any, isDeleted: Option[Boolean]): MSeq[Any] = {
    MSeq(rowId, columnId, colType, isDeleted.orNull, null, null, null,
      null, null, null, null, null, null, null, null, null, null)
  }

  def filterByCols(spark: SparkSession, changelist: DataFrame, cols: Seq[String]): DataFrame = {
    val dCols = spark.sparkContext.broadcast(cols.toSet)
    val columnIdIdx = changelist.columns.indexOf(ColumnId)
    changelist.filter(row => dCols.value.contains(row.getString(columnIdIdx)))
  }
}
