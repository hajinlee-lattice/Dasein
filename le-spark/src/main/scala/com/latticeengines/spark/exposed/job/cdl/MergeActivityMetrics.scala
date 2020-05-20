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

import scala.collection.JavaConversions._

class MergeActivityMetrics extends AbstractSparkJob[MergeActivityMetricsJobConfig] {

  val ENTITY_ID_COLS: Seq[String] = Seq(AccountId.name, ContactId.name)

  override def runJob(spark: SparkSession, lattice: LatticeContext[MergeActivityMetricsJobConfig]): Unit = {
    val config: MergeActivityMetricsJobConfig = lattice.config
    val input = lattice.input
    val inputMetadata = config.inputMetadata.getMetadata
    val mergedTableLabels: Seq[String] = config.mergedTableLabels.iterator.toSeq

    val outputMetadata: ActivityStoreSparkIOMetadata = new ActivityStoreSparkIOMetadata()
    val detailsMap = new util.HashMap[String, Details]() // mergedTableLabel -> details
    var mergedIdx: Int = 0
    var mergedTables: Seq[DataFrame] = Seq()
    mergedTableLabels.foreach((mergedTableLabel: String) => {
      val details: Details = new Details()
      details.setStartIdx(mergedIdx)
      mergedIdx += 1

      val (entity: BusinessEntity, servingEntity: TableRoleInCollection) = parseLabel(mergedTableLabel)
      val (output: DataFrame, deprecateAttrs: Seq[String]) = mergeGroup(input, inputMetadata.get(mergedTableLabel), entity, inputMetadata.get(servingEntity.name))
      mergedTables :+= output
      details.setLabels(deprecateAttrs)
      detailsMap.put(mergedTableLabel, details)
    })
    outputMetadata.setMetadata(detailsMap)
    lattice.outputStr = serializeJson(outputMetadata)
    lattice.output = mergedTables.toList
  }

  private def mergeGroup(input: List[DataFrame], details: Details, entity: BusinessEntity, activeMetricsDetails: Details): (DataFrame, Seq[String]) = {
    val start = details.getStartIdx
    val end = details.getStartIdx + details.getLabels.size
    val groupDFsToMerge: Seq[DataFrame] = input.slice(start, end)
    val entityIdCol: String = DeriveAttrsUtils.getEntityIdColumnNameFromEntity(entity)
    var merged: DataFrame = groupDFsToMerge.reduce((df1, df2) => df1.join(df2, Seq(entityIdCol), "fullouter"))

    // merge active metrics and identify attributes to deprecate
    if (activeMetricsDetails != null) {
      val activeMetrics: DataFrame = input(activeMetricsDetails.getStartIdx)
      val newMetricsAttrs = merged.columns
      val missingAttrs = activeMetrics.columns.diff(newMetricsAttrs)
      val activeColumns = entityIdCol +: missingAttrs
      merged = merged.join(activeMetrics.select(activeColumns.head, activeColumns.tail: _*), Seq(entityIdCol), "leftouter").na.fill(0)
      (merged, missingAttrs)
    } else {
      (merged, Seq())
    }
  }

  private def parseLabel(mergedTableLabel: String): (BusinessEntity, TableRoleInCollection) = {
    val comb: Seq[String] = mergedTableLabel.split("_")
    val entity: String = comb.head
    val servingEntity: String = comb(1)
    (BusinessEntity.getByName(entity), TableRoleInCollection.getByName(servingEntity))
  }
}
