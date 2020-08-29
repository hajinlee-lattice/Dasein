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
        assert(txnStreams.size == 2)
        val transformed: Seq[DataFrame] = txnStreams.map(txnStream => renameFieldsAndAddPeriodName(txnStream, renameMapping, null))
        // should be only two transformed: Spending & Analytic
        MergeUtils.concat2(transformed.head, transformed(1))
      } else {
        // build aggregated period txn
        // inputs ordered by period names. 2 productTypes for each period
        assert(txnStreams.size == lattice.config.inputPeriods.size * 2)
        var fragments: Seq[DataFrame] = Seq()
        for (i <- lattice.config.inputPeriods.indices) {
          val periodName: String = lattice.config.inputPeriods(i)
          val typeMerged: DataFrame = MergeUtils.concat2(txnStreams(i * 2), txnStreams(i * 2 + 1))
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
