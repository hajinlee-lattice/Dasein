package com.latticeengines.spark.exposed.job.dcp

import java.util

import com.latticeengines.common.exposed.util.JsonUtils
import com.latticeengines.domain.exposed.dcp.{DataReport, DataReportMode}
import com.latticeengines.domain.exposed.spark.dcp.RollupDataReportConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class RollupDataReportJob extends AbstractSparkJob[RollupDataReportConfig] {
  override def runJob(spark: SparkSession, lattice: LatticeContext[RollupDataReportConfig]): Unit = {
    val config = lattice.config
    val inputs = lattice.input
    val matchedDunsAttr = config.getMatchedDunsAttr
    val mode = config.getMode

    if (mode == DataReportMode.RECOMPUTE_ROOT) {
      val (dupReport, dunsCntDF) =  generateDunsCount(inputs.toSet, matchedDunsAttr)
      lattice.output = dunsCntDF :: Nil
      lattice.outputStr = JsonUtils.serialize(dupReport :: Nil)
    } else {
      val inputOwnerIdToIndex = config.getInputOwnerIdToIndex.asScala.toMap
      val parentIdToChildren : Map[String, util.Set[String]]= config.getParentIdToChildren.asScala.toMap
      val updatedOwnerIds : List[String] = config.getUpdatedOwnerIds.asScala.toList
      val updatedOwnerIdToTarget = new mutable.HashMap[String, DataFrame]
      val targets : ListBuffer[DataFrame] = ListBuffer()
      val dupReports : ListBuffer[DataReport.DuplicationReport] = ListBuffer()
      for (updatedOwnerId <- updatedOwnerIds) {
        val childIds: util.Set[String] = parentIdToChildren(updatedOwnerId)
        var childSet: Set[DataFrame] = Set()
        for (childOwnerId <- childIds.asScala) {
          if (!updatedOwnerIds.contains(childOwnerId) && inputOwnerIdToIndex.contains(childOwnerId)) {
            val index: Integer = inputOwnerIdToIndex(childOwnerId)
            childSet += inputs(index)
          } else if (updatedOwnerIdToTarget.contains(childOwnerId)) {
            childSet += updatedOwnerIdToTarget(childOwnerId)
          } else {
            logSpark(s"this shouldn't happen for $childOwnerId")
          }
        }
        val (dupReport, dunsCntDF) = generateDunsCount(childSet, matchedDunsAttr)
        updatedOwnerIdToTarget.put(updatedOwnerId, dunsCntDF)
        dupReports.append(dupReport)
        targets.append(dunsCntDF)
      }
      lattice.output = targets.toList
      lattice.outputStr = JsonUtils.serialize(dupReports.toList.asJava)
    }
  }

  private def generateDunsCount(childSet : Set[DataFrame], matchedDunsAttr : String) :  (DataReport.DuplicationReport,
    DataFrame) = {
    val result : DataFrame = childSet.reduce((x, y) => x union y)
    val dunsCntDF: DataFrame =  result.groupBy(matchedDunsAttr).agg(sum("cnt").alias("cnt"))
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
}
