package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.spark.cdl.ChangeListConfig
import com.latticeengines.spark.exposed.job.{ AbstractSparkJob, LatticeContext }
import com.latticeengines.spark.util.MergeUtils
import org.apache.spark.sql.{ DataFrame, SparkSession, Row }
import org.apache.spark.sql.functions.{ col, first }
import scala.collection.JavaConverters._
import org.apache.spark.sql.types._

class MergeChangeListJob extends AbstractSparkJob[ChangeListConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[ChangeListConfig]): Unit = {

    import spark.implicits._
    val config: ChangeListConfig = lattice.config
    val changeList = lattice.input.head
    val source: DataFrame = if (lattice.input.size == 2) lattice.input(1) else null
    val joinKey = config.getJoinKey

    val deletedRowIds = changeList
      .filter(col("RowId").isNotNull && col("ColumnId").isNull && col("Deleted") === true)
      .select(col("RowId").alias(joinKey)).distinct
    val deletedColumnIds = changeList
      .filter(col("RowId").isNull && col("ColumnId").isNotNull && col("Deleted") === true)
      .select(col("ColumnId")).distinct
    val deletedColumnCount = deletedColumnIds.count()
    val deletedColumnNames = if (deletedColumnCount > 0) deletedColumnIds.as[String].collect.toList else List()
    val updatedChangeList = changeList.filter(col("RowId").isNotNull && col("ColumnId").isNotNull && col("Deleted") =!= true)

    if (source == null) {
      lattice.output = pivotChangeList(updatedChangeList, joinKey) :: Nil
    } else {
      val result = mergeWithExisting(source, deletedRowIds, deletedColumnNames, updatedChangeList, joinKey);
      lattice.output = result :: Nil
    }
  }

  def mergeWithExisting(source: DataFrame, deletedRowIds: DataFrame, deletedColumnNames: List[String],
    updatedChangeList: DataFrame, joinKey: String): DataFrame = {
    var result = source
    
    if (deletedColumnNames.size > 0) {
      result = result.drop(deletedColumnNames: _*)
    }
    
    val deletedRowCount = deletedRowIds.count()
    if (deletedRowCount > 0) {
      result = MergeUtils.joinWithMarkers(result, deletedRowIds, Seq(joinKey), "left")
      val (fromMarker, toMarker) = MergeUtils.getJoinMarkers()
      result = result.filter(col(toMarker).isNull)
      result = result.drop(Seq(fromMarker, toMarker): _*)
    }

    val updatedRowCount = updatedChangeList.count()
    if (updatedRowCount > 0) {
      val pivotedChangeList = pivotChangeList(updatedChangeList, joinKey)
      result = MergeUtils.merge2(result, pivotedChangeList, Seq(joinKey), Set(), overwriteByNull = false)
    }
    result
  }

  def pivotChangeList(df: DataFrame, joinKey: String): DataFrame = {
    val dataTypes = List("String", "Boolean", "Integer", "Long", "Float", "Double")
    val resultDf = df.filter(col("DataType") === dataTypes(0))
    var result = pivotTypeChangeList(resultDf, dataTypes(0), joinKey)      
    for (i <- 1 until dataTypes.length) {
      var nextDf = df.filter(col("DataType") === dataTypes(i))
      var newDfCount = nextDf.count()
      if (newDfCount > 0) {
        var nextResult = pivotTypeChangeList(nextDf, dataTypes(i), joinKey)
        result = MergeUtils.merge2(result, nextResult, Seq(joinKey), Set(), overwriteByNull = false)
      }
    }
    result
  }

  def pivotTypeChangeList(df: DataFrame, dataType: String, joinKey: String): DataFrame = {
    df.groupBy(col("RowId").alias(joinKey)) //
      .pivot("ColumnId") //
      .agg(first("To" + dataType))
  }
}

