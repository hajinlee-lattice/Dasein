package com.latticeengines.spark.exposed.job.common

import com.latticeengines.domain.exposed.spark.common.ChangeListConstants.{ColumnId, Deleted, RowId}
import com.latticeengines.domain.exposed.spark.common.GetRowChangesConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
  * Get RowId of new and deleted rows
  */
class GetRowChangesJob extends AbstractSparkJob[GetRowChangesConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[GetRowChangesConfig]): Unit = {
    val input: DataFrame = lattice.input.head
    val rowChanges = input.filter(col(ColumnId).isNull).select(RowId, Deleted).persist(StorageLevel.DISK_ONLY)
    val newRows = rowChanges.filter(col(Deleted).isNull || col(Deleted) === false).select(RowId)
    val deletedRows = rowChanges.filter(col(Deleted) === true).select(RowId)
    lattice.output = List(newRows, deletedRows)
  }
}
