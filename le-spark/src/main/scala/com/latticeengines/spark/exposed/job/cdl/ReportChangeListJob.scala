package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.spark.cdl.ChangeListConfig
import com.latticeengines.spark.exposed.job.{ AbstractSparkJob, LatticeContext }
import org.apache.spark.sql.{ DataFrame, SparkSession, Row }
import org.apache.spark.sql.functions.{ col }
import scala.collection.JavaConverters._
import org.apache.spark.sql.types._

class ReportChangeListJob extends AbstractSparkJob[ChangeListConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[ChangeListConfig]): Unit = {

    val config: ChangeListConfig = lattice.config
    val input = lattice.input.head

    val allRowIds = input
        .filter(col("RowId").isNotNull)
        .select(col("RowId").alias("AllRowIds")).distinct
    val newRowIds = input
        .filter(col("RowId").isNotNull && col("ColumnId").isNull && (col("Deleted").isNull || col("Deleted") === false))
        .select(col("RowId").alias("NewRowIds")).distinct
    val deletedRowIds = input
        .filter(col("RowId").isNotNull && col("ColumnId").isNull && col("Deleted") === true)
        .select(col("RowId").alias("DeletedRowIds")).distinct
    var updated = allRowIds.join(newRowIds, allRowIds("AllRowIds") === newRowIds("NewRowIds"), "left")
                      .join(deletedRowIds, allRowIds("AllRowIds") === deletedRowIds("DeletedRowIds"), "left")
    updated = updated.filter(col("NewRowIds").isNull && col("DeletedRowIds").isNull)
    
    val newCount= newRowIds.count
    val updatedCount= updated.count
    val deletedCount= deletedRowIds.count
    
    val row = Row(newCount, updatedCount, deletedCount)
    val schema = getOutputSchema()
    val rows = Seq(row)
    val result = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    lattice.output = result :: Nil
  }
  
  private def getOutputSchema(): StructType = {
    StructType(
      StructField("NewRecords", LongType, nullable = true) ::
      StructField("UpdatedRecords", LongType, nullable = true) ::
      StructField("DeletedRecords", LongType, nullable = true) :: Nil)
  }
}

