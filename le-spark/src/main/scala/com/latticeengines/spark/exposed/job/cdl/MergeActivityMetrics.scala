package com.latticeengines.spark.exposed.job.cdl

import java.util

import com.latticeengines.domain.exposed.metadata.InterfaceName.{AccountId, ContactId}
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection
import com.latticeengines.domain.exposed.query.BusinessEntity
import com.latticeengines.domain.exposed.spark.cdl.ActivityStoreSparkIOMetadata.Details
import com.latticeengines.domain.exposed.spark.cdl.{ActivityStoreSparkIOMetadata, MergeActivityMetricsJobConfig}
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.DeriveAttrsUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters.asScalaIteratorConverter

class MergeActivityMetrics extends AbstractSparkJob[MergeActivityMetricsJobConfig] {

  val ENTITY_ID_COLS: Seq[String] = Seq(AccountId.name, ContactId.name)

  override def runJob(spark: SparkSession, lattice: LatticeContext[MergeActivityMetricsJobConfig]): Unit = {
    val config: MergeActivityMetricsJobConfig = lattice.config
    val input = lattice.input
    val inputMetadata = config.inputMetadata.getMetadata
    val mergedTableLabels: Seq[String] = asScalaIteratorConverter(config.mergedTableLabels.iterator).asScala.toSeq

    val outputMetadata: ActivityStoreSparkIOMetadata = new ActivityStoreSparkIOMetadata()
    val detailsMap = new util.HashMap[String, Details]() // mergedTableLabel -> details
    var mergedIdx: Int = 0
    var mergedTables: Seq[DataFrame] = Seq()
    mergedTableLabels.foreach((mergedTableLabel: String) => {
      val details: Details = new Details()
      details.setStartIdx(mergedIdx)
      detailsMap.put(mergedTableLabel, details)
      mergedIdx += 1

      mergedTables :+= mergeGroup(input, inputMetadata.get(mergedTableLabel), mergedTableLabel)
    })
    outputMetadata.setMetadata(detailsMap)

    lattice.outputStr = serializeJson(outputMetadata)
    lattice.output = mergedTables.toList
  }

  private def mergeGroup(input: List[DataFrame], details: Details, mergedTableLabel: String): DataFrame = {
    val (entity: BusinessEntity, servingEntity: TableRoleInCollection) = parseLabel(mergedTableLabel)
    val start = details.getStartIdx
    val end = details.getStartIdx + details.getLabels.size
    val groupDFsToMerge: Seq[DataFrame] = input.slice(start, end)
    val joinCol: String = DeriveAttrsUtils.getMetricsGroupEntityIdColumnName(entity)
    val merged: DataFrame = groupDFsToMerge.reduce((df1, df2) => df1.join(df2, Seq(joinCol), "fullouter"))
    // TODO - deal with null values for other use cases
    // for web visit use case, null -> no visit records found -> 0
    // this may not be true for other use cases
    merged.na.fill(0)
  }

  private def parseLabel(mergedTableLabel: String): (BusinessEntity, TableRoleInCollection) = {
    val comb: Seq[String] = mergedTableLabel.split("_")
    val entity: String = comb.head
    val servingEntity: String = comb(1)
    (BusinessEntity.getByName(entity), TableRoleInCollection.getByName(servingEntity))
  }
}
