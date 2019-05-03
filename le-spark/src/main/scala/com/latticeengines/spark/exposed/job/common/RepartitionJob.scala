package com.latticeengines.spark.exposed.job.common

import com.latticeengines.domain.exposed.spark.common.RepartitionConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.commons.collections4.CollectionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import scala.collection.JavaConverters._

class RepartitionJob extends AbstractSparkJob[RepartitionConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[RepartitionConfig]): Unit = {

    val config = lattice.config
    val input = lattice.input.head

    val partitions = config.getPartitions

    val partitioned =
      if (partitions < 200) {
        input
      } else {
        if (CollectionUtils.isNotEmpty(config.getPartitionKeys)) {
          val pKeys = config.getPartitionKeys.asScala.toList.map(col)
            input.repartition(partitions, pKeys: _*)
        } else {
          input.repartition(partitions)
        }
      }

    lattice.output = partitioned :: Nil

  }

}

