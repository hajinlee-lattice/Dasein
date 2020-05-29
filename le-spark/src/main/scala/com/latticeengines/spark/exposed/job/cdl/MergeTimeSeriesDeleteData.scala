package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.InterfaceName.TimeRanges
import com.latticeengines.domain.exposed.spark.cdl.MergeTimeSeriesDeleteDataConfig
import com.latticeengines.spark.DeleteUtils
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{collect_set, lit, udf}

import scala.collection.JavaConverters._
import scala.collection.mutable

class MergeTimeSeriesDeleteData extends AbstractSparkJob[MergeTimeSeriesDeleteDataConfig] {

  private val TIME_RANGE_TEMP_COL = "timeRange"

  override def runJob(spark: SparkSession, lattice: LatticeContext[MergeTimeSeriesDeleteDataConfig]): Unit = {
    val config = lattice.config
    val timeRanges = config.timeRanges.asScala.mapValues(p => Array(Long2long(p.get(0)), Long2long(p.get(1))))

    val serializeRanges = udf {
      ranges: mutable.WrappedArray[mutable.WrappedArray[Long]] =>
        DeleteUtils.serializeTimeRanges(ranges)
    }
    val mergedDeleteData = (0 until config.numberOfDeleteInputs)
      .map(idx => {
        val df = lattice.input(idx).select(config.joinKey)
        // use [ long.min, long.max ] to replace null for easier processing
        val range = timeRanges.getOrElse(idx, Array(Long.MinValue, Long.MaxValue))
        df.filter(df(config.joinKey).isNotNull)
          .withColumn(TIME_RANGE_TEMP_COL, lit(range))
      })
      .reduce((accDf, df) => accDf.unionByName(df))
      .groupBy(config.joinKey)
      .agg(collect_set(TIME_RANGE_TEMP_COL).as(TimeRanges.name))
    val timeRangesSerialized = mergedDeleteData.withColumn(
      TimeRanges.name, serializeRanges(mergedDeleteData.col(TimeRanges.name)))

    lattice.output = timeRangesSerialized :: Nil
  }
}
