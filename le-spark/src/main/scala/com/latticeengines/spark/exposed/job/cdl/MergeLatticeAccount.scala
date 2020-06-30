package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.spark.cdl.MergeLatticeAccountConfig
import com.latticeengines.domain.exposed.spark.common.CopyConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.CopyUtils
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

/**
  * MergeLatticeAccount is different from normal Upsert or Merge job in that
  * 1. The LatticeAccount table is extremely wide
  * 2. The new table has all the columns, due to the fact that match api is not a columnar store
  *
  * Therefore, merging LatticeAccount can be simply take new table as a whole,
  * then union with the missing rows from old table and align the schema to the new table
  */
class MergeLatticeAccount extends AbstractSparkJob[MergeLatticeAccountConfig] {

  private val AccountId = InterfaceName.AccountId.name
  private val new_marker = "__new__"

  override def runJob(spark: SparkSession, lattice: LatticeContext[MergeLatticeAccountConfig]): Unit = {
    val oldTbl = lattice.input.head
    val newTbl = lattice.input(1)

    val newIds = newTbl.select(AccountId).withColumn(new_marker, lit(true)).checkpoint()
    val rollover = oldTbl
      .join(newIds, Seq(AccountId), joinType = "left")
      .filter(col(new_marker).isNull)
      .drop(new_marker)

    val newTblWithTs = CopyUtils.fillTimestamps(newTbl)
    val filtered = filterOldTbl(newTblWithTs, rollover)

    val result = newTblWithTs unionByName filtered
    lattice.output = List(result)
  }

  private def filterOldTbl(newTbl: DataFrame, oldTbl: DataFrame): DataFrame = {
    val filteredCols: Seq[String] = oldTbl.columns.intersect(newTbl.columns)
    val filtered = if (filteredCols.length < oldTbl.columns.length) {
      val copyConfig = new CopyConfig
      copyConfig.setSelectAttrs(filteredCols.asJava)
      CopyUtils.copy(copyConfig, Seq(oldTbl))
    } else {
      oldTbl
    }
    val newCols = newTbl.columns.diff(filtered.columns)
    if (newCols.isEmpty) {
      filtered
    } else {
      val fields = newTbl.schema.fields.filter(f => newCols.contains(f.name))
      fields.foldLeft(filtered)((df, f) => df.withColumn(f.name, lit(null).cast(f.dataType)))
    }
  }

}
