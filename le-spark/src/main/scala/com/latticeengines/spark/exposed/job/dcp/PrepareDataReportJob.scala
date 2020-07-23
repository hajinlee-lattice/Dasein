package com.latticeengines.spark.exposed.job.dcp

import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate
import com.latticeengines.domain.exposed.spark.dcp.PrepareDataReportConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

class PrepareDataReportJob extends AbstractSparkJob[PrepareDataReportConfig] {
  override def runJob(spark: SparkSession, lattice: LatticeContext[PrepareDataReportConfig]): Unit = {
    val inputs: List[DataFrame] = lattice.input
    val config: PrepareDataReportConfig = lattice.config
    val matchedDunsAttr = config.getMatchedDunsAttr
    val classificationAttr = config.getClassificationAttr

    val targets : ListBuffer[DataFrame] = ListBuffer()
    for (input <- inputs) {
      val accepted = DnBMatchCandidate.Classification.Accepted.name
      val acceptedDF = input.filter(col(classificationAttr) === accepted)
      val dunsCntDF: DataFrame = acceptedDF.groupBy(matchedDunsAttr).agg(count("*").alias("cnt"))
      targets.append(dunsCntDF)
    }
    lattice.output = targets.toList
  }
}
