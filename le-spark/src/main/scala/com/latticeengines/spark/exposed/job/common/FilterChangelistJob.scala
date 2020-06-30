package com.latticeengines.spark.exposed.job.common

import com.latticeengines.domain.exposed.spark.common.ChangeListConstants.{ColumnId, Deleted, RowId, ToString}
import com.latticeengines.domain.exposed.spark.common.FilterChangelistConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConversions._

class FilterChangelistJob extends AbstractSparkJob[FilterChangelistConfig] {
  override def runJob(spark: SparkSession, lattice: LatticeContext[FilterChangelistConfig]): Unit = {
    val config: FilterChangelistConfig = lattice.config
    val selectColumns = config.getSelectColumns.toSeq
    val key = config.getKey
    val columnId = config.getColumnId
    val changelist: DataFrame = lattice.input.head

    val changed = changelist.filter(col(ColumnId) === columnId && (col(Deleted).isNull || col(Deleted) === false))
    val deleted = changelist.filter(col(ColumnId) === columnId && col(Deleted) === true)

    val changedRenamed = changed
      .withColumnRenamed(ToString, columnId)
      .withColumnRenamed(RowId, key)
      .select(selectColumns map col: _*)
      .distinct()

    lattice.output = List(changedRenamed, deleted)
  }
}
