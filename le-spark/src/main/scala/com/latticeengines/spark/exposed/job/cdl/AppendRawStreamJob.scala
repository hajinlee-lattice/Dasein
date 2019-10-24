package com.latticeengines.spark.exposed.job.cdl

import java.time.Instant
import java.time.temporal.ChronoUnit

import com.latticeengines.common.exposed.util.DateTimeUtils.{dateToDayPeriod, toDateOnlyFromMillis}
import com.latticeengines.domain.exposed.metadata.InterfaceName.{__StreamDate, __StreamDateId}
import com.latticeengines.domain.exposed.spark.cdl.AppendRawStreamConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.MergeUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Process activity stream imports and merge with current batch store if any
  */
class AppendRawStreamJob extends AbstractSparkJob[AppendRawStreamConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[AppendRawStreamConfig]): Unit = {
    // prepare input/vars
    val config: AppendRawStreamConfig = lattice.config
    val hasImport = config.matchedRawStreamInputIdx != null
    val hasMaster = config.masterInputIdx != null
    // partition by dateId
    setPartitionTargets(0, Seq(__StreamDateId.name()), lattice)

    // calculation
    var df: DataFrame = if (hasImport) {
      val getDate = udf {
        time: Long => toDateOnlyFromMillis(time.toString)
      }
      val getDateId = udf {
        time: Long => dateToDayPeriod(toDateOnlyFromMillis(time.toString))
      }

      // add date & dateId
      var mdf = lattice.input(config.matchedRawStreamInputIdx)
      mdf = mdf.withColumn(__StreamDate.name, getDate(mdf.col(config.dateAttr)))
        .withColumn(__StreamDateId.name, getDateId(mdf.col(config.dateAttr)))
      if (hasMaster) {
        mdf = MergeUtils.concat2(mdf, lattice.input(config.masterInputIdx))
      }
      mdf
    } else {
      lattice.input(config.masterInputIdx)
    }
    if (config.retentionDays != null) {
      // apply retention policy and remove old data
      df = df.filter(df.col(__StreamDateId.name).geq(getStartDateId(config.retentionDays, config.currentEpochMilli)))
    }

    lattice.output = df :: Nil
  }

  private def getStartDateId(retentionDays: Int, epochMilli: Long): Int = {
    val startTime = Instant.ofEpochMilli(epochMilli).minus(retentionDays, ChronoUnit.DAYS).toEpochMilli.toString
    dateToDayPeriod(toDateOnlyFromMillis(startTime))
  }
}
