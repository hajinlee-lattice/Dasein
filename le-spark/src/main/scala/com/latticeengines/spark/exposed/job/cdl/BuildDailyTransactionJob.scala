package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.spark.cdl.TransformTxnStreamConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.{MergeUtils, TransactionSparkUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConversions._

class BuildDailyTransactionJob extends AbstractSparkJob[TransformTxnStreamConfig] {
  override def runJob(spark: SparkSession, lattice: LatticeContext[TransformTxnStreamConfig]): Unit = {
    val dailyStreams = lattice.input
    val compositeSrc: Seq[String] = lattice.config.compositeSrc
    val targetCols: Seq[String] = lattice.config.targetColumns
    val renameMapping: Map[String, String] = lattice.config.renameMapping.toMap
    val retainTypes: Seq[String] = lattice.config.retainTypes

    assert(dailyStreams.size == retainTypes.size)

    val transformed: Seq[DataFrame] = dailyStreams.map(stream => TransactionSparkUtils.renameFieldsAndAddPeriodName(stream, renameMapping, null))
    val merged: DataFrame = transformed.reduce(MergeUtils.concat2)
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
  }
}
