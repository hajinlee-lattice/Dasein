package com.latticeengines.spark.exposed.job.dcp

import com.latticeengines.common.exposed.util.JsonUtils
import com.latticeengines.domain.exposed.spark.dcp.InputPresenceConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._
import scala.collection.mutable

class InputPresenceJob extends AbstractSparkJob[InputPresenceConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[InputPresenceConfig]): Unit = {
    val result : DataFrame = lattice.input.head
    val config = lattice.config
    val inputNames : Set[String] = config.getInputNames.asScala.toSet
    val statsMap : mutable.Map[String, Long] = mutable.Map.empty[String, Long]

    inputNames.foreach(name => {
      if (result.columns.contains(name)) {
        val populated: DataFrame = result.filter(col(name).isNotNull && regexp_replace(col(name), " ", "") =!= "")
          .persist
        StorageLevel.DISK_ONLY
        val populatedCnt = populated.count()
        statsMap  += (name -> populatedCnt)
      } else {
        statsMap += (name -> 0)
      }
    })

    lattice.outputStr = JsonUtils.serialize(statsMap.asJava)
  }

}
