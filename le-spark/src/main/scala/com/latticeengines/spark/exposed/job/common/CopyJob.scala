package com.latticeengines.spark.exposed.job.common

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.spark.common.CopyConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.commons.collections4.MapUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

import scala.collection.JavaConverters._

/**
  * Copy a single dataset, can perform simple column selection and rename
  * Operation sequence: select -> drop -> rename -> add create/update timestamps
  */
class CopyJob extends AbstractSparkJob[CopyConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[CopyConfig]): Unit = {
    val config: CopyConfig = lattice.config
    val input = lattice.input.head

    val colsToSelect: List[String] =
      if (config.getSelectAttrs == null)
        List()
      else
        config.getSelectAttrs.asScala.toList.intersect(input.columns)
    val selected =
      if (colsToSelect.isEmpty) {
        input
      } else {
        input.select(colsToSelect map col: _*)
      }

    val colsToDrop: List[String] =
      if (config.getDropAttrs == null)
        List()
      else
        config.getDropAttrs.asScala.toList.intersect(selected.columns)
    val dropped =
      if (colsToDrop.isEmpty) {
        selected
      } else {
        selected.drop(colsToDrop: _*)
      }

    val renamed =
      if (MapUtils.isEmpty(config.getRenameAttrs)) {
        dropped
      } else {
        val attrsToRename: Map[String, String] = config.getRenameAttrs.asScala.toMap
          .filterKeys(dropped.columns.contains(_))
        val newAttrs = dropped.columns.map(c => attrsToRename.getOrElse(c, c))
        dropped.toDF(newAttrs: _*)
      }

    val result =
      if (config.isAddTimestampAttrs) {
        val nowMills = System.currentTimeMillis()
        renamed
          .withColumn(InterfaceName.CDLCreatedTime.name(), lit(nowMills))
          .withColumn(InterfaceName.CDLUpdatedTime.name(), lit(nowMills))
      } else {
        renamed
      }

    lattice.output = result :: Nil
  }

}
