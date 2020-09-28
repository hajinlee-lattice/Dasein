package com.latticeengines.spark.exposed.job.dcp

import com.latticeengines.common.exposed.util.JsonUtils
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate
import com.latticeengines.domain.exposed.dcp.DataReport
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit
import com.latticeengines.domain.exposed.spark.dcp.SplitImportMatchResultConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.{CSVUtils, CountryCodeUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions.{col, count, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._

class SplitImportMatchResultJob extends AbstractSparkJob[SplitImportMatchResultConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[SplitImportMatchResultConfig]): Unit = {
    val config: SplitImportMatchResultConfig = lattice.config
    val input: DataFrame = lattice.input.head

    val countryAttr: String = config.getCountryAttr
    val url: String = config.getManageDbUrl
    val user: String = config.getUser
    val password: String = config.getPassword
    val encryptionKey: String = config.getEncryptionKey
    val saltHint: String = config.getSaltHint
    val totalCnt: Long = config.getTotalCount
    val geoReport = generateGeoReport(input, countryAttr, totalCnt, url, user, password, encryptionKey, saltHint)

    val ccAttr = config.getConfidenceCodeAttr
    val matchToDUNSReport = generateMatchToDunsReport(input, ccAttr, totalCnt)

    val classificationAttr: String = config.getClassificationAttr
    val matchedDunsAttr: String = config.getMatchedDunsAttr
    val acceptedAttrs: Map[String, String] = config.getAcceptedAttrsMap.asScala.toMap
    val rejectedAttrs: Map[String, String] = config.getRejectedAttrsMap.asScala.toMap

    val (acceptedDF, acceptedCsv) = filterAccepted(input, classificationAttr, acceptedAttrs)
    val rejectedCsv = filterRejected(input, classificationAttr, rejectedAttrs)
    val (dupReport, dunsCount) = generateDupReport(acceptedDF, matchedDunsAttr)

    val report : DataReport = new DataReport
    report.setGeoDistributionReport(geoReport)
    report.setDuplicationReport(dupReport)
    report.setMatchToDUNSReport(matchToDUNSReport)

    lattice.outputStr = JsonUtils.serialize(report)
    lattice.output = acceptedCsv :: rejectedCsv :: dunsCount :: Nil
  }

  private def filterAccepted(input: DataFrame, classificationAttr: String, acceptedAttrs: Map[String, String]):
  (DataFrame, DataFrame) = {
    val accepted = DnBMatchCandidate.Classification.Accepted.name
    val acceptedDF = input.filter(col(classificationAttr) === accepted)
    (acceptedDF, selectAndRename(acceptedDF, acceptedAttrs))
  }

  private def filterRejected(input: DataFrame, matchIndicator: String, rejectedAttrs: Map[String, String]): DataFrame = {
    val rejected = DnBMatchCandidate.Classification.Rejected.name
    selectAndRename(input.filter(col(matchIndicator).isNull || col(matchIndicator) === rejected), rejectedAttrs)
  }

  private def generateGeoReport(input: DataFrame, countryAttr: String, totalCnt: Long, url: String, user: String,
                                password: String, key: String, salt: String): DataReport.GeoDistributionReport = {
    val geoReport: DataReport.GeoDistributionReport = new DataReport.GeoDistributionReport
    if (input.columns.contains(countryAttr)) {
      // fake one country code column
      val countryCodeAttr: String = "CountryCodeAttr"
      val countryDF = CountryCodeUtils.convert(input, countryAttr, countryCodeAttr, url, user, password, key, salt)
        .groupBy(col(countryCodeAttr))
        .agg(count("*").alias("cnt"))
      countryDF.collect().foreach(row => {
        val countryCodeVal: String = row.getAs(countryCodeAttr)
        val countryCode: String = if (StringUtils.isEmpty(countryCodeVal)) "indeterminate" else countryCodeVal
        val count: Long = row.getAs("cnt")
        geoReport.addGeoDistribution(countryCode, count, totalCnt)
      })
    }
    geoReport
  }

  private def generateDupReport(acceptedDF: DataFrame, matchedDunsAttr: String): (DataReport.DuplicationReport,
    DataFrame) = {
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
    (dupReport, dunsCntDF)
  }

  private def generateMatchToDunsReport(input: DataFrame, cc: String, totalCnt: Long): DataReport.MatchToDUNSReport = {
    val matchToDunsReport = new DataReport.MatchToDUNSReport
    if (input.columns.contains(cc)) {
      val cntDF: DataFrame = input.groupBy(cc).agg(count("*").alias("cnt")).persist(StorageLevel.DISK_ONLY)
      cntDF.collect().foreach(row => {
        val ccVal: Int = row.getAs(cc)
        val ccCnt: Long = row.getAs("cnt")
        if (ccVal == 0) {
          matchToDunsReport.setNoMatchCnt(ccCnt)
        }
        matchToDunsReport.addConfidenceItem(ccVal, ccCnt, totalCnt)
      })
    }
    matchToDunsReport
  }

  private def selectAndRename(input: DataFrame, attrNames: Map[String, String]): DataFrame = {
    val sequenceValues = attrNames.values.toList
    logSpark("the display names in map  are " + JsonUtils.serialize(sequenceValues))
    val selected = input.columns.filter(attrNames.keySet)
    val filtered = input.select(selected map col: _*)
    val newNames = filtered.columns.map(c => attrNames.getOrElse(c, c))
    logSpark("the unordered names  " + JsonUtils.serialize(newNames))
    val orderedNames = newNames.sortWith((n1, n2) => sequenceValues.indexOf(n1) < sequenceValues.indexOf(n2))
    logSpark("the ordered names in csv are " + JsonUtils.serialize(newNames))
    filtered.toDF(orderedNames: _*)
  }

  override def finalizeJob(spark: SparkSession, latticeCtx: LatticeContext[SplitImportMatchResultConfig]): List[HdfsDataUnit] = {
    val units: List[HdfsDataUnit] = CSVUtils.dfToCSV(spark, compress=false, latticeCtx.targets.take(2), latticeCtx
      .output.take(2))
    units ::: super.finalizeJob(spark, latticeCtx.targets.drop(2), latticeCtx.output.drop(2))
  }

}
