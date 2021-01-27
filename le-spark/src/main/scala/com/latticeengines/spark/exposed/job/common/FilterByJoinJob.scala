package com.latticeengines.spark.exposed.job.common

import com.latticeengines.domain.exposed.spark.common.FilterByJoinConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

class FilterByJoinJob extends AbstractSparkJob[FilterByJoinConfig] {
  private val inputPrefix = "Input_"

  override def runJob(spark: SparkSession, lattice: LatticeContext[FilterByJoinConfig]): Unit = {
    val config: FilterByJoinConfig = lattice.config
    val switchSide = config.getSwitchSide != null && config.getSwitchSide
    var source: DataFrame = lattice.input.head
    var input: DataFrame = if (lattice.input.size == 1) null else lattice.input(1)
    val key = config.getKey
    val selectColumns = if (config.getSelectColumns == null) null else config.getSelectColumns.asScala.toList
    val joinType = config.getJoinType

    val columns = if (selectColumns == null) source.columns.toList else selectColumns
    var output: DataFrame = null
    if (input != null) {
      // Rename columns in the input data (except join key) to avoid conflicts on column names
      var renamed: DataFrame = input
      if (switchSide) {
        renamed = source
        source = input
        input = renamed
      }
      input.columns.foreach(column => {
        if (!column.equalsIgnoreCase(key)) {
          renamed = renamed.withColumnRenamed(column, inputPrefix + column)
        }
      })
      output = source.select(columns map col: _*).join(renamed, Seq(key), joinType).select(columns map col: _*)
    } else {
      output = source.select(columns map col: _*)
    }

    lattice.output = output :: Nil
  }
}
