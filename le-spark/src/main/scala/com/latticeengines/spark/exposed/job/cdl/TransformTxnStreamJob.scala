package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.spark.cdl.TransformTxnStreamConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.MergeUtils
import org.apache.commons.collections4.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, struct, udf}
import org.apache.spark.sql.types.StringType

import scala.collection.JavaConversions._

/**
 * from aggregated transaction stream, derive standard aggregated transaction store schema
 */
class TransformTxnStreamJob extends AbstractSparkJob[TransformTxnStreamConfig] {
  override def runJob(spark: SparkSession, lattice: LatticeContext[TransformTxnStreamConfig]): Unit = {
    val txnStreams: Seq[DataFrame] = lattice.input
    val compositeSrc: Seq[String] = lattice.config.compositeSrc
    val targetCols: Seq[String] = lattice.config.targetColumns
    val renameMapping: Map[String, String] = lattice.config.renameMapping.toMap

    val transformed: DataFrame =
      if (CollectionUtils.isEmpty(lattice.config.inputPeriods)) {
        // build aggregated txn
        if (CollectionUtils.isEmpty(lattice.config.retainTypes)) {
          assert(txnStreams.size == 2)
        } else {
          assert(txnStreams.size == 1)
        }
        val transformed: Seq[DataFrame] = txnStreams.map(txnStream => renameFieldsAndAddPeriodName(txnStream, renameMapping, null))
        transformed.reduce((t1, t2) => MergeUtils.concat2(t1, t2))
      } else {
        // build aggregated period txn
        // inputs ordered by period names
        val defaultRetainTypes: Boolean = {
          if (CollectionUtils.isEmpty(lattice.config.retainTypes)) {
            assert(txnStreams.size == lattice.config.inputPeriods.size * 2)
            true
          } else {
            assert(txnStreams.size == lattice.config.inputPeriods.size)
            false
          }
        }
        var fragments: Seq[DataFrame] = Seq()
        for (i <- lattice.config.inputPeriods.indices) {
          val periodName: String = lattice.config.inputPeriods(i)
          val typeMerged: DataFrame = {
            if (defaultRetainTypes) {
              MergeUtils.concat2(txnStreams(i * 2), txnStreams(i * 2 + 1))
            } else {
              txnStreams(i)
            }
          }
          fragments :+= renameFieldsAndAddPeriodName(typeMerged, renameMapping, periodName)
        }
        fragments.reduce((f1, f2) => MergeUtils.concat2(f1, f2))
      }
    val generateCompKey: UserDefinedFunction = udf((row: Row) => row.mkString(""))
    val withCompositeKey: DataFrame = transformed.withColumn(InterfaceName.__Composite_Key__.name, generateCompKey(struct(compositeSrc.map(col): _*)))
    lattice.output = withCompositeKey.select(targetCols.head, targetCols.tail: _*) :: Nil
    if (StringUtils.isNotBlank(lattice.config.partitionKey)) {
      setPartitionTargets(0, Seq(lattice.config.partitionKey), lattice)
    }
  }

  def renameFieldsAndAddPeriodName(df: DataFrame, renameMapping: Map[String, String], periodName: String): DataFrame = {
    renameMapping.foldLeft(df) { (df, entry) =>
      val (src, target) = (entry._1, entry._2)
      df.withColumnRenamed(src, target)
        .withColumn(InterfaceName.PeriodName.name, lit(periodName).cast(StringType))
    }
  }
}
