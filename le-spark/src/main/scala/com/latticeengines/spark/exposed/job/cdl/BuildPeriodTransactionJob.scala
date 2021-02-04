package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit
import com.latticeengines.domain.exposed.spark.cdl.SparkIOMetadataWrapper.Partition
import com.latticeengines.domain.exposed.spark.cdl.TransformTxnStreamConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.{MergeUtils, TransactionSparkUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConversions._

class BuildPeriodTransactionJob extends AbstractSparkJob[TransformTxnStreamConfig] {
  override def runJob(spark: SparkSession, lattice: LatticeContext[TransformTxnStreamConfig]): Unit = {
    val periodStreams: Seq[DataFrame] = lattice.input
    val compositeSrc: Seq[String] = lattice.config.compositeSrc
    val targetCols: Seq[String] = lattice.config.targetColumns
    val renameMapping: Map[String, String] = lattice.config.renameMapping.toMap
    val inputPartitions: Map[String, Partition] = lattice.config.inputMetadataWrapper.getMetadata.toMap // periodName -> productTypeStreams
    assert(periodStreams.size == getExpectedStreamCount(inputPartitions))

    val merged: DataFrame = inputPartitions.map(partition => {
      val periodName = partition._1
      val streams: Seq[DataFrame] = periodStreams.subList(partition._2.getStartIdx, partition._2.getStartIdx + partition._2.getLabels.size)
      val typeMerged: DataFrame = streams.reduce(MergeUtils.concat2)
      TransactionSparkUtils.renameFieldsAndAddPeriodName(typeMerged, renameMapping, periodName)
    }).reduce(MergeUtils.concat2)

    val withCompositeKey: DataFrame = merged.withColumn(InterfaceName.__Composite_Key__.name, TransactionSparkUtils.generateCompKey(compositeSrc))
    val cleaned: DataFrame = withCompositeKey.select(targetCols.head, targetCols.tail: _*)

    val result: DataFrame = {
      if (StringUtils.isNotBlank(lattice.config.repartitionKey)) {
        cleaned.repartition(200, col(lattice.config.repartitionKey))
      } else {
        cleaned
      }
    }

    if (StringUtils.isNotBlank(lattice.config.partitionKey)) {
      setPartitionTargets(0, Seq(lattice.config.partitionKey), lattice)
    }
    lattice.output = result :: Nil
    if (lattice.config.outputParquet) {
      lattice.targets.head.setDataFormat(DataUnit.DataFormat.PARQUET)
    }
  }

  def getExpectedStreamCount(partitions: Map[String, Partition]): Int = {
    partitions.map(partition => partition._2.getLabels.size).sum
  }
}
