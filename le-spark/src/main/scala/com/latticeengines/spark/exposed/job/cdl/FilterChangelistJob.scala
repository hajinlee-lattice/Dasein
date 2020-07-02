package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.spark.cdl.FilterChangelistConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

class FilterChangelistJob extends AbstractSparkJob[FilterChangelistConfig] {
  override def runJob(spark: SparkSession, lattice: LatticeContext[FilterChangelistConfig]): Unit = {
    val config: FilterChangelistConfig = lattice.config
    val selectColumns = config.getSelectColumns.asScala.toSeq
    val key = config.getKey
    val columnId = config.getColumnId
    val changelist: DataFrame = lattice.input.head

    val changed = changelist.filter(col("ColumnId") === columnId && col("Deleted").isNull)
    val deleted = changelist.filter(col("ColumnId") === columnId && col("Deleted") === true)

    val changedRenamed = changed.withColumnRenamed("ToString", columnId).withColumnRenamed("RowId", key).select(selectColumns map col: _*).distinct()

    var output: List[DataFrame] = List(changedRenamed, deleted)

    lattice.output = output
  }
}
