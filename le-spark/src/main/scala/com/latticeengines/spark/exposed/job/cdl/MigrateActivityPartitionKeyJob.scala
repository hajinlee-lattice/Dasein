package com.latticeengines.spark.exposed.job.cdl

import java.util

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.spark.cdl.SparkIOMetadataWrapper.Partition
import com.latticeengines.domain.exposed.spark.cdl.{MigrateActivityPartitionKeyJobConfig, SparkIOMetadataWrapper}
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConversions._

class MigrateActivityPartitionKeyJob extends AbstractSparkJob[MigrateActivityPartitionKeyJobConfig] {

  val TARGET_PARTITION_KEY_MAP: Map[String, String] = Map(
    InterfaceName.__StreamDateId.name -> InterfaceName.StreamDateId.name
  )

  override def runJob(spark: SparkSession, lattice: LatticeContext[MigrateActivityPartitionKeyJobConfig]): Unit = {
    val inputMetadata = lattice.config.inputMetadata.getMetadata // streamId -> details[origin partition keys]

    val outputMetadata: SparkIOMetadataWrapper = new SparkIOMetadataWrapper
    val detailsMap = new util.HashMap[String, Partition] // streamId -> details[migrated partition keys]
    var migrated: Seq[DataFrame] = Seq()
    lattice.config.inputMetadata.getMetadata.foreach(entry => {
      val details: Partition = new Partition
      details.setStartIdx(migrated.size)
      details.setLabels(new util.ArrayList[String])
      val (streamId: String, labels: util.List[String]) = (entry._1, entry._2.getLabels)
      for (offset <- labels.indices) {
        val originalKey: String = labels(offset)
        val targetKey: String = getTargetPartitionKey(originalKey)
        val ioIndex = inputMetadata(streamId).getStartIdx + offset
        val df: DataFrame = lattice.input(ioIndex)
        migrated :+= appendTargetPartitionKey(df, originalKey, targetKey)
        details.getLabels.add(targetKey)
        detailsMap.put(streamId, details)
        setPartitionTargets(ioIndex, Seq(targetKey), lattice)
      }
    })
    outputMetadata.setMetadata(detailsMap)

    lattice.output = migrated.toList
    // output metadata: streamId -> [migrated partition key]
    lattice.outputStr = serializeJson(outputMetadata)
  }

  def appendTargetPartitionKey(df: DataFrame, originalKey: String, targetPartitionKey: String): DataFrame = {
    // simply append new partition key for idempotence
    if (df.columns.contains(targetPartitionKey)) {
      df
    } else {
      df.withColumn(targetPartitionKey, df(originalKey)).drop(df(originalKey))
    }
  }

  private def getTargetPartitionKey(origin: String): String = {
    if (!TARGET_PARTITION_KEY_MAP.contains(origin)) {
      throw new UnsupportedOperationException(s"Unable to find target key to migrate $origin to")
    }
    TARGET_PARTITION_KEY_MAP(origin)
  }
}
