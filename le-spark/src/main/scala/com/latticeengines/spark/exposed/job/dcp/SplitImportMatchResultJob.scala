package com.latticeengines.spark.exposed.job.dcp

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit
import com.latticeengines.domain.exposed.spark.dcp.SplitImportMatchResultConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.CSVUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

class SplitImportMatchResultJob extends AbstractSparkJob[SplitImportMatchResultConfig] {

  val matchIndicator: String = InterfaceName.LatticeAccountId.name

  override def runJob(spark: SparkSession, lattice: LatticeContext[SplitImportMatchResultConfig]): Unit = {
    val config: SplitImportMatchResultConfig = lattice.config
    val input: DataFrame = lattice.input.head

    val acceptedAttrs: Map[String, String] = config.getAcceptedAttrsMap.asScala.toMap
    val rejectedAttrs: Map[String, String] = config.getRejectedAttrsMap.asScala.toMap

    val accepted = filterAccepted(input, acceptedAttrs)
    val rejected = filterRejected(input, rejectedAttrs)

    lattice.output = accepted :: rejected :: Nil
  }

  private def filterAccepted(input: DataFrame, acceptedAttrs: Map[String, String]): DataFrame = {
    selectAndRename(input.filter(col(matchIndicator).isNotNull), acceptedAttrs)
  }

  private def filterRejected(input: DataFrame, rejectedAttrs: Map[String, String]): DataFrame = {
    selectAndRename(input.filter(col(matchIndicator).isNull), rejectedAttrs)
  }

  private def selectAndRename(input: DataFrame, attrNames: Map[String, String]): DataFrame = {
    val selected = input.columns.filter(attrNames.keySet)
    val filtered = input.select(selected map col: _*)
    val newNames = filtered.columns.map(c => attrNames.getOrElse(c, c))
    filtered.toDF(newNames: _*)
  }

  override def finalizeJob(spark: SparkSession, latticeCtx: LatticeContext[SplitImportMatchResultConfig]): List[HdfsDataUnit] = {
    CSVUtils.dfToCSV(spark, compress=false, latticeCtx.targets, latticeCtx.output)
  }

}
