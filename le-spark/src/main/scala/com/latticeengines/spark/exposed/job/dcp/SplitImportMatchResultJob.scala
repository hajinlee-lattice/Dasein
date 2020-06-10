package com.latticeengines.spark.exposed.job.dcp

import com.latticeengines.common.exposed.util.JsonUtils
import com.latticeengines.domain.exposed.dcp.DunsStats
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit
import com.latticeengines.domain.exposed.spark.dcp.SplitImportMatchResultConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.CSVUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._

class SplitImportMatchResultJob extends AbstractSparkJob[SplitImportMatchResultConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[SplitImportMatchResultConfig]): Unit = {
    val config: SplitImportMatchResultConfig = lattice.config
    val input: DataFrame = lattice.input.head

    val matchedDunsAttr: String = config.getMatchedDunsAttr
    val acceptedAttrs: Map[String, String] = config.getAcceptedAttrsMap.asScala.toMap
    val rejectedAttrs: Map[String, String] = config.getRejectedAttrsMap.asScala.toMap

    val (acceptedDF, acceptedCsv) = filterAccepted(input, matchedDunsAttr, acceptedAttrs)
    val rejectedCsv = filterRejected(input, matchedDunsAttr, rejectedAttrs)
    val dunsCntDF: DataFrame =  acceptedDF.groupBy(matchedDunsAttr).agg(count("*").alias("cnt"))
    val uniqueCnt = dunsCntDF.filter(col("cnt") === 1).agg(sum("cnt").cast("long")).first().getLong(0)
    val duplicatedCnt = dunsCntDF.filter(col("cnt") > 1).agg(sum("cnt").cast("long")).first().getLong(0)
    val totalCnt = uniqueCnt + duplicatedCnt
    val distinctCount = dunsCntDF.count()
    val duns = new DunsStats
    duns.setDistinctCnt(distinctCount)
    duns.setUniqueCnt(uniqueCnt)
    duns.setDuplicatedCnt(duplicatedCnt)
    duns.setUniPercent(uniqueCnt * 100.0 / totalCnt)
    duns.setDupPercent(duplicatedCnt * 100.0 / totalCnt)

    lattice.outputStr = JsonUtils.serialize(duns)
    lattice.output = acceptedCsv :: rejectedCsv :: Nil
  }

  private def filterAccepted(input: DataFrame, matchIndicator: String, acceptedAttrs: Map[String, String]):
  (DataFrame, DataFrame) = {
    val acceptedDF = input.filter(col(matchIndicator).isNotNull && col(matchIndicator) =!= "").persist(StorageLevel.DISK_ONLY)
    (acceptedDF, selectAndRename(acceptedDF, acceptedAttrs))
  }

  private def filterRejected(input: DataFrame, matchIndicator: String, rejectedAttrs: Map[String, String]): DataFrame = {
    selectAndRename(input.filter(col(matchIndicator).isNull || col(matchIndicator) === ""), rejectedAttrs)
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
