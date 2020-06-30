package com.latticeengines.spark.exposed.job.common

import com.latticeengines.domain.exposed.spark.common.ChangeListConstants.{ColumnId, Deleted, RowId}
import com.latticeengines.domain.exposed.spark.common.{ColumnChanges, GetColumnChangesConfig}
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

/**
  * Get updated columns and delete columns from a change list
  */
class GetColumnChangesJob extends AbstractSparkJob[GetColumnChangesConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[GetColumnChangesConfig]): Unit = {
    val input: DataFrame = lattice.input.head.select(ColumnId, RowId, Deleted)
    val config: GetColumnChangesConfig = lattice.config

    val includeAttrs: Set[String] = if (config.getIncludeAttrs == null) Set() else config.getIncludeAttrs.asScala.toSet
    val changelist = if (includeAttrs.isEmpty) {
      input
    } else {
      val bCastIncAttrs = spark.sparkContext.broadcast(includeAttrs)
      input.filter(row => bCastIncAttrs.value.contains(row.getAs[String](ColumnId)))
    }

    val removed = changelist.filter(col(Deleted) === true && col(RowId).isNull).distinct()
    val changed = changelist.filter(col(RowId).isNotNull).select(ColumnId).groupBy(ColumnId).count()

    val result = new ColumnChanges
    result.setChanged(changed.collect().map(r => //
      (r.getString(0).toString, r.getLong(1).asInstanceOf[java.lang.Long]) //
    ).toMap.asJava)
    result.setRemoved(removed.collect().map(r => r.getString(0)).toList.asJava)

    lattice.outputStr = serializeJson(result)
  }
}
