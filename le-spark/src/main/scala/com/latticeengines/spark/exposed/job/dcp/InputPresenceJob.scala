package com.latticeengines.spark.exposed.job.dcp

import java.util
import java.util.concurrent.ConcurrentHashMap

import com.latticeengines.common.exposed.util.JsonUtils
import com.latticeengines.domain.exposed.spark.dcp.InputPresenceConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

class InputPresenceJob extends AbstractSparkJob[InputPresenceConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[InputPresenceConfig]): Unit = {
    val result : DataFrame = lattice.input.head
    val config = lattice.config
    val inputNames : Set[String] = config.getInputNames.asScala.toSet
    val statsMap: util.Map[String, Long] = new ConcurrentHashMap
    val excludeEmpty: Boolean = if (lattice.config.getExcludeEmpty == null) false else lattice.config.getExcludeEmpty

    inputNames.par.map(name => {
      if (result.columns.contains(name)) {
        val populated: DataFrame =
          if (excludeEmpty) {
            result.filter(col(name).isNotNull && col(name).notEqual(""))
          } else {
            result.filter(col(name).isNotNull)
          }
        val populatedCnt = populated.count()
        statsMap.put(name, populatedCnt)
      } else {
        statsMap.put(name, 0)
      }
    })

    lattice.outputStr = JsonUtils.serialize(statsMap)
  }

}
