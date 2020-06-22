package com.latticeengines.spark.exposed.job.dcp

import com.latticeengines.common.exposed.util.JsonUtils
import com.latticeengines.domain.exposed.dcp.DataReport
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit
import com.latticeengines.domain.exposed.spark.dcp.SplitImportMatchResultConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.CSVUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions.{col, count, sum, rand, round}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._

class SplitImportMatchResultJob extends AbstractSparkJob[SplitImportMatchResultConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[SplitImportMatchResultConfig]): Unit = {
    val config: SplitImportMatchResultConfig = lattice.config
    val input: DataFrame = lattice.input.head

    val matchedCountryAttr: String = config.getMatchedCountryAttr
    val countryCodeName: String = config.getCountryCode
    val totalCnt: Long = config.getTotalCount
    val geoReport = generateGeoReport(input, matchedCountryAttr, countryCodeName, totalCnt)

    val cc = config.getConfidenceCode
    val matchToDUNSReport = generateMatchToDunsReport(input, cc, totalCnt)

    val matchedDunsAttr: String = config.getMatchedDunsAttr
    val acceptedAttrs: Map[String, String] = config.getAcceptedAttrsMap.asScala.toMap
    val rejectedAttrs: Map[String, String] = config.getRejectedAttrsMap.asScala.toMap

    val (acceptedDF, acceptedCsv) = filterAccepted(input, matchedDunsAttr, acceptedAttrs)
    val rejectedCsv = filterRejected(input, matchedDunsAttr, rejectedAttrs)
    val dupReport = generateDupReport(acceptedDF, matchedDunsAttr)

    val report : DataReport = new DataReport
    report.setGeoDistributionReport(geoReport)
    report.setDuplicationReport(dupReport)
    report.setMatchToDUNSReport(matchToDUNSReport)

    lattice.outputStr = JsonUtils.serialize(report)
    lattice.output = acceptedCsv :: rejectedCsv :: Nil
  }

  private def filterAccepted(input: DataFrame, matchIndicator: String, acceptedAttrs: Map[String, String]):
  (DataFrame, DataFrame) = {
    val acceptedDF = input.filter(col(matchIndicator).isNotNull && col(matchIndicator) =!= "")
    (acceptedDF, selectAndRename(acceptedDF, acceptedAttrs))
  }

  private def filterRejected(input: DataFrame, matchIndicator: String, rejectedAttrs: Map[String, String]): DataFrame = {
    selectAndRename(input.filter(col(matchIndicator).isNull || col(matchIndicator) === ""), rejectedAttrs)
  }

  private def generateGeoReport(input: DataFrame, matchedCountryAttr: String, countryCodeName: String,
                                totalCnt: Long):  DataReport.GeoDistributionReport = {
    val geoReport: DataReport.GeoDistributionReport = new DataReport.GeoDistributionReport
    if (input.columns.contains(matchedCountryAttr)) {
      // fake one country code column
      val countryDF = input.withColumn(countryCodeName, col(matchedCountryAttr))
        .groupBy(col(matchedCountryAttr), col(countryCodeName))
        .agg(count("*").alias("cnt"))
        .persist(StorageLevel.DISK_ONLY)
      countryDF.collect().foreach(row => {
        val countryVal: String = row.getAs(matchedCountryAttr)
        val country: String = if (StringUtils.isEmpty(countryVal)) "undefined" else countryVal
        val countryCodeVal: String = row.getAs(countryCodeName)
        val countryCode: String = if (StringUtils.isEmpty(countryCodeVal)) "undefined" else countryCodeVal
        val count: Long = row.getAs("cnt")
        geoReport.addGeoDistribution(countryCode, country, count, totalCnt)
      })
    }
    geoReport
  }

  private def generateDupReport(acceptedDF: DataFrame, matchedDunsAttr: String): DataReport.DuplicationReport = {
    val dunsCntDF: DataFrame =  acceptedDF.groupBy(matchedDunsAttr).agg(count("*").alias("cnt"))
      .persist(StorageLevel.DISK_ONLY).checkpoint()
    val uniqueDF: DataFrame = dunsCntDF.filter(col("cnt") === 1)
    val uniqueCnt = if (uniqueDF == null) 0 else uniqueDF.count()
    val duplicateDF: DataFrame = dunsCntDF.filter(col("cnt") > 1)
    val duplicatedCnt = if (duplicateDF == null || duplicateDF.head(1).isEmpty) 0 else duplicateDF.agg(sum("cnt").cast("long")).first().getLong(0)
    val distinctCount = dunsCntDF.count()
    val dupReport = new DataReport.DuplicationReport
    dupReport.setDistinctRecords(distinctCount)
    dupReport.setUniqueRecords(uniqueCnt)
    dupReport.setDuplicateRecords(duplicatedCnt)
    dupReport
  }

  private def generateMatchToDunsReport(input: DataFrame, cc: String, totalCnt: Long): DataReport.MatchToDUNSReport = {
    val matchToDunsReport = new DataReport.MatchToDUNSReport
    val modifiedDF = if (!input.columns.contains(cc)) input.withColumn(cc, round(rand * 10).cast("integer")) else input
    val cntDF: DataFrame = modifiedDF.groupBy(cc).agg(count("*").alias("cnt")).persist(StorageLevel.DISK_ONLY)
    cntDF.collect().foreach(row => {
      val ccVal: Int = row.getAs(cc)
      val ccCnt: Long = row.getAs("cnt")
      if (ccVal == 0) {
        matchToDunsReport.setNoMatchCnt(ccCnt)
      }
      matchToDunsReport.addConfidenceItem(ccVal, ccCnt, totalCnt)
    })
    matchToDunsReport
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
