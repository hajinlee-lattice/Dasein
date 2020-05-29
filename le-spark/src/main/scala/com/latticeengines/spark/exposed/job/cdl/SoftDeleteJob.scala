package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.spark.cdl.SoftDeleteConfig
import com.latticeengines.spark.DeleteUtils
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters.{asScalaBufferConverter, asScalaIteratorConverter}
import scala.collection.mutable.ListBuffer

class SoftDeleteJob extends AbstractSparkJob[SoftDeleteConfig] {
  override def runJob(spark: SparkSession, lattice: LatticeContext[SoftDeleteConfig]): Unit = {
    val config: SoftDeleteConfig = lattice.config
    val deleteSrcIdx: Int = if (config.getDeleteSourceIdx == null) 1 else config.getDeleteSourceIdx.toInt
    val originalSrcIdx: Int = (deleteSrcIdx + 1) % 2
    val joinColumn = config.getIdColumn
    val sourceIdColumn = config.getSourceIdColumn
    val hasPartitionKey = config.getPartitionKeys != null && config.getPartitionKeys.size() > 0
    val needPartitionOutput: Boolean = config.getNeedPartitionOutput == true

    if (hasPartitionKey && needPartitionOutput) {
      setPartitionTargets(0, asScalaIteratorConverter(config.getPartitionKeys.iterator).asScala.toSeq, lattice)
    }

    val delete: DataFrame = lattice.input(deleteSrcIdx)
    val original: DataFrame = lattice.input(originalSrcIdx)

    var dropFields : ListBuffer[String] = ListBuffer()
    if (!needPartitionOutput && hasPartitionKey) {
      dropFields ++= asScalaBufferConverter(config.getPartitionKeys).asScala
    }

    // calculation
    var result: DataFrame = null
    if (joinColumn.equals(sourceIdColumn)) {
      result = original.alias("original")
        .join(delete, Seq(joinColumn), "left")
        .where(retainRecord(config, delete, original, joinColumn))
        .select("original.*")
    } else {
      result = original.alias("original")
        .withColumn(joinColumn, original.col(sourceIdColumn))
        .join(delete, Seq(joinColumn), "left")
        .where(retainRecord(config, delete, original, joinColumn))
        .select("original.*")
    }

    if (dropFields.nonEmpty) {
      result = result.drop(dropFields:_*)
    }

    // finish
    lattice.output = result::Nil
  }

  private def retainRecord(config: SoftDeleteConfig, deleteData: DataFrame, sourceToDelete: DataFrame, joinColumn: String) = {
    val deleteByTimeRange = StringUtils.isNotBlank(config.getEventTimeColumn) && StringUtils.isNotBlank(config.getTimeRangesColumn)
    if (deleteByTimeRange) {
      val notInAnyTimeRange = udf((time: Long, timeRangesStr: String) => {
        // null/empty means no delete time range configured (and hence return true)
        DeleteUtils
          .deserializeTimeRanges(timeRangesStr)
          .forall(_.forall(range => range(0) > time || range(1) < time))
      })
      val time = sourceToDelete.col(config.getEventTimeColumn)
      val timeRanges = deleteData.col(config.getTimeRangesColumn)
      deleteData.col(joinColumn).isNull.or(notInAnyTimeRange(time, timeRanges))
    } else {
      deleteData.col(joinColumn).isNull
    }
  }
}
