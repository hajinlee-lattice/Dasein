package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.spark.cdl.ChangeListConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.MergeUtils
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import scala.collection.JavaConverters._
import org.apache.spark.sql.types._
import scala.collection.mutable.{Map => MMap, Seq => MSeq}

class CreateChangeListJob extends AbstractSparkJob[ChangeListConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[ChangeListConfig]): Unit = {
    val config: ChangeListConfig = lattice.config
    val toTable = lattice.input.head
    val fromTable: DataFrame = if (lattice.input.size == 2) lattice.input(1) else null
    val joinKey = config.getJoinKey
    var exclusionCols = config.getExclusionColumns.asScala.toSet 
    val params = new Params
    params.fromTableColPos = if (fromTable != null) fromTable.columns.view.zipWithIndex.toMap else null
    params.toTableColPos = toTable.columns.view.zipWithIndex.toMap
    params.fromTableColType = if (fromTable != null) getColTypeMap(fromTable) else null
    params.toTableColType = getColTypeMap(toTable)
    params.outputSchema = getOutputSchema()
    params.outputColPos = getOutputColPosMap(params.outputSchema)
    
    val result = 
      if (fromTable == null) {
        createForNewTable(toTable, exclusionCols, joinKey, params)
      } else {
        createChangeList(spark, toTable, fromTable, exclusionCols, joinKey, params)
      }
    
    lattice.output = result :: Nil
  }

  private def createForNewTable(toTable: DataFrame, exclusionCols: Set[String], joinKey: String, params: Params): DataFrame = {
    val cols = toTable.columns.filter(c => !exclusionCols.contains(c))
    val changeList = toTable.flatMap(row => {
      var rows: MSeq[Option[Row]] =  cols map (colName => {
        Helper.buildNewRowColumnList(row, colName, joinKey, params.toTableColPos, params.toTableColPos,
            params.toTableColType, params.outputColPos)
      })
      val rowList = Helper.buildNewRowList(row, joinKey, params.toTableColPos)
      rows = rowList +: rows 
      rows.flatten
    })(RowEncoder(params.outputSchema))
    changeList    
  }
  
  private def getColTypeMap(df: DataFrame) : MMap[String, String] = {
    var colTypeMap = MMap[String, String]()
    df.dtypes.foreach { f =>
        val colName = f._1
        val colType = f._2
        colTypeMap(colName) = colType
    }
    colTypeMap
  }  
  
  private def getOutputColPosMap(schema: StructType) : Map[String, Int] = {
    val colNames = schema map { f =>
        f.name
    }
    colNames.view.zipWithIndex.toMap
  }  
  
  private def createChangeList(spark: SparkSession, toTable: DataFrame, fromTable: DataFrame,
      exclusionCols: Set[String], joinKey: String, params: Params): DataFrame = {
    
    val commonColNames = fromTable.columns.intersect(toTable.columns).diff(Seq(joinKey).union(exclusionCols.toSeq))
    val deletedColNames = fromTable.columns.diff(toTable.columns.union(exclusionCols.toSeq))
    val newColNames = toTable.columns.diff(fromTable.columns.union(exclusionCols.toSeq))

    val deletedCols = Helper.buildDeletedColumnList(deletedColNames.toList)
    val deletedColList = spark.createDataFrame(spark.sparkContext.parallelize(deletedCols), params.outputSchema)
    val joined = MergeUtils.joinWithMarkers(fromTable, toTable, Seq(joinKey), "outer")
    val (fromColPos, toColPos) = MergeUtils.getColPosOnBothSides(joined)
    val (fromMarker, toMarker) = MergeUtils.getJoinMarkers()
    
    val changedList = joined.flatMap(row => {
      val isDeletedRow = row.getAs(fromMarker) != null && row.getAs(toMarker) == null
      val isNewRow = row.getAs(fromMarker) == null && row.getAs(toMarker) != null
      var rows: MSeq[Option[Row]] = MSeq();
      if (isDeletedRow) {
        rows = rows :+ Helper.buildDeletedRowList(row, joinKey, fromColPos)
      } else if (isNewRow) {
        rows = rows :+ Helper.buildNewRowList(row, joinKey, fromColPos)
      }
      if (isDeletedRow) {
        val newRows = deletedColNames.union(commonColNames) map (colName => {
          Helper.buildDeletedRowColumnList(row, colName, joinKey, fromColPos, params.fromTableColType, params.outputColPos)
        })
        rows = rows ++ newRows
      } else if (isNewRow) {
        val newRows = newColNames.union(commonColNames) map (colName => {
          Helper.buildNewRowColumnList(row, colName, joinKey, fromColPos, toColPos,
            params.toTableColType, params.outputColPos)
        })
        rows = rows ++ newRows
      } else {
        val newRows1 = deletedColNames map (colName => {
          Helper.buildDeletedRowColumnList(row, colName, joinKey, fromColPos, params.fromTableColType, params.outputColPos)
        })
        rows = rows ++ newRows1
        val newRows2 = commonColNames map (colName => {
          Helper.buildCommonRowColumnList(row, colName, joinKey, 
            fromColPos, toColPos, params.toTableColType, params.outputColPos)
        })
        rows = rows ++ newRows2
        val newRows3 = newColNames map (colName => {
          Helper.buildNewRowColumnList(row, colName, joinKey, fromColPos, toColPos,
            params.toTableColType, params.outputColPos)
        })
        rows = rows ++ newRows3
      }
      rows.flatten
    })(RowEncoder(params.outputSchema))
    
    changedList.union(deletedColList)
  }
  
  private def getOutputSchema(): StructType = {
    StructType(
      StructField("RowId", StringType, nullable = true) ::
      StructField("ColumnId", StringType, nullable = true) ::
      StructField("DataType", StringType, nullable = true) ::
      StructField("Deleted", BooleanType, nullable = true) ::
      StructField("FromString", StringType, nullable = true) ::
      StructField("ToString", StringType, nullable = true) ::
      StructField("FromBoolean", BooleanType, nullable = true) ::
      StructField("ToBoolean", BooleanType, nullable = true) ::
      StructField("FromInteger", IntegerType, nullable = true) ::
      StructField("ToInteger", IntegerType, nullable = true) ::
      StructField("FromLong", LongType, nullable = true) ::
      StructField("ToLong", LongType, nullable = true) ::
      StructField("FromFloat", FloatType, nullable = true) ::
      StructField("ToFloat", FloatType, nullable = true) ::
      StructField("FromDouble", DoubleType, nullable = true) ::
      StructField("ToDouble", DoubleType, nullable = true) :: Nil)
  }
}

class Params extends Serializable {
  var fromTableColPos: Map[String, Int] = _
  var toTableColPos: Map[String, Int] = _
  var fromTableColType: MMap[String, String] = _
  var toTableColType: MMap[String, String] = _
  var outputSchema: StructType = _
  var outputColPos: Map[String, Int] = _
}

object Helper extends Serializable {
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
    
    def buildDeletedColumnList(columns: List[String]) : Seq[Row] = {
      columns.map( col => {
        val record = getRowTemplate(null, col, null, Some(true))
        Row.fromSeq(record)        
      })
    } 
    
    def buildDeletedRowColumnList(row: Row, colName: String, joinKey: String, fromColPos: Map[String, Int],
        fromTableColType: MMap[String, String], outputColPos: Map[String, Int]) : Option[Row] = {
      val rowId = row.get(fromColPos(joinKey))
      val value = row.get(fromColPos(colName))
      if (value == null) {
        return None
      }
      var colType = fromTableColType(colName).dropRight(4)
      val record = getRowTemplate(rowId, colName, colType, Some(true))
      val fromCol = "From" + colType
      record(outputColPos(fromCol)) = value            
      Some(Row.fromSeq(record))
    }
    
    def buildNewRowColumnList(row: Row, colName: String, joinKey: String, fromColPos: Map[String, Int], toColPos: Map[String, Int], 
        toTableColType: MMap[String, String], 
        outputColPos: Map[String, Int]) : Option[Row] = {

      val rowId = row.get(fromColPos(joinKey))
      val value = row.get(toColPos(colName))
      if (value == null) {
        return None
      }
      var colType = toTableColType(colName).dropRight(4)
      val record = getRowTemplate(rowId, colName, colType, None)
      val toCol = "To" + colType
      record(outputColPos(toCol)) = value
      Some(Row.fromSeq(record))
    }
        
    def buildCommonRowColumnList(row: Row, colName: String, joinKey: String, 
      fromColPos: Map[String, Int], toColPos: Map[String, Int], toTableColType: MMap[String, String], 
      outputColPos: Map[String, Int]) : Option[Row] = {
      val fromVal = row.get(fromColPos(colName))
      val toVal = row.get(toColPos(colName))
      if (fromVal == null && toVal == null) {
        return None
      }
      if (fromVal != null && toVal != null && fromVal == toVal) {
        return None;
      }
      val rowId = row.get(fromColPos(joinKey))
      var colType = toTableColType(colName).dropRight(4)
      val record = getRowTemplate(rowId, colName, colType, None)
      val fromCol = "From" + colType
      record(outputColPos(fromCol)) = fromVal
      val toCol = "To" + colType
      record(outputColPos(toCol)) = toVal
          
      Some(Row.fromSeq(record))    
    }
    
    def getRowTemplate(rowId: Any, columnId: Any, colType: Any, isDeleted: Option[AnyVal]): MSeq[Any] = {
      MSeq(rowId, columnId, colType, isDeleted.getOrElse(null), null, null, null, 
          null, null, null, null, null, null, null, null, null, null)
    }
}

