package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.metadata.InterfaceName.{CDLUpdatedTime, LastActivityDate}
import com.latticeengines.domain.exposed.spark.cdl.GenerateCuratedAttributesConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.MergeUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Merge curated attributes from multiple sources and do some consolidation
  */
class GenerateCuratedAttributes extends AbstractSparkJob[GenerateCuratedAttributesConfig] {
  override def runJob(spark: SparkSession, lattice: LatticeContext[GenerateCuratedAttributesConfig]): Unit = {
    val config: GenerateCuratedAttributesConfig = lattice.config
    val masterTableIdx = Option(config.masterTableIdx)
    val lastActivityDateIdx = Option(config.lastActivityDateInputIdx)
    val attrsToMerge = config.attrsToMerge.asScala.mapValues(_.asScala)
    val specialJoinKeys = config.joinKeys.asScala
    val templateValues = config.templateValueMap.asScala

    // calculation
    val optLastActivityDf = if (lastActivityDateIdx.isDefined) {
      val lastActivityDf = lattice.input(config.lastActivityDateInputIdx)
      Some(masterTableIdx
        .map(lattice.input(_))
        .map(mergeLastActivityDate(lastActivityDf, _, config.joinKey, config.columnsToIncludeFromMaster.asScala.toList))
        .getOrElse(lastActivityDf.select(config.joinKey, LastActivityDate.name)))
    } else {
      None: Option[DataFrame]
    }

    val mdf: DataFrame = if (attrsToMerge.isEmpty) {
      // must have val
      optLastActivityDf.get
    } else {
      val mergeFn = (accDf: DataFrame, args: (Integer, mutable.Map[String, String])) => {
        val srcJoinKey = specialJoinKeys.getOrElse(args._1, config.joinKey)
        val df = modifyJoinKey(lattice.input(args._1), config.joinKey, srcJoinKey)
        MergeUtils.merge2(
          accDf, mergeAttr(df, args._2, templateValues).select(config.joinKey, args._2.valuesIterator.toSeq: _*),
          Seq(config.joinKey), Set(), overwriteByNull = false)
      }
      optLastActivityDf match {
        case Some(df) => attrsToMerge.foldLeft(df)(mergeFn(_, _))
        case _ =>
          val inputIdx = attrsToMerge.head._1
          val srcJoinKey = specialJoinKeys.getOrElse(inputIdx, config.joinKey)
          val df = mergeAttr(modifyJoinKey(lattice.input(inputIdx), config.joinKey, srcJoinKey), attrsToMerge.head._2, templateValues)
            .select(config.joinKey, attrsToMerge.head._2.valuesIterator.toSeq: _*)
          attrsToMerge.tail.foldLeft(df)(mergeFn(_, _))
      }
    }

    lattice.output = mdf :: Nil
  }

  private def modifyJoinKey(df: DataFrame, tgtJoinKey: String, srcJoinKey: String): DataFrame = {
    val cols = df.columns.toSeq
    if (cols.contains(tgtJoinKey) || tgtJoinKey.equals(srcJoinKey)) {
      df
    } else {
      df.withColumnRenamed(srcJoinKey, tgtJoinKey)
    }
  }

  private def mergeAttr(df: DataFrame, attrs: mutable.Map[String, String], templateValues: mutable.Map[String, String]): DataFrame = {
    val mapTemplateValue = udf((s: String) => templateValues.getOrElse(s, null))
    attrs.foldLeft(df)((accDf, args) => {
      val tgtAttr = args._2
      val srcAttr = args._1
      val cols = accDf.columns.toSeq
      if (cols.contains(tgtAttr)) {
        // TODO consider failing job
        accDf
      } else if (!cols.contains(srcAttr)) {
        // TODO pass in tgt attribute type, use attr name to support time field for now
        if (tgtAttr.endsWith("Date") || tgtAttr.endsWith("Time")) {
          accDf.withColumn(tgtAttr, lit(null).cast(LongType))
        } else {
          accDf.withColumn(tgtAttr, lit(null).cast(StringType))
        }
      } else if (InterfaceName.CDLCreatedTemplate.name().equals(srcAttr)) {
        accDf.withColumn(tgtAttr, mapTemplateValue(accDf.col(srcAttr)))
      } else {
        accDf.withColumn(tgtAttr, accDf.col(srcAttr))
      }
    })
  }

  private def mergeLastActivityDate(lastActivityDf: DataFrame, masterDf: DataFrame, joinKey: String, additionalColumns: List[String]): DataFrame = {
    val colsFromMaster = additionalColumns ::: List(joinKey).intersect(masterDf.columns)
    masterDf.join(lastActivityDf, Seq(joinKey), "left")
      .select(colsFromMaster.map(col) ::: List(coalesce(
        // pick last activity date first, then last update time
        lastActivityDf.col(LastActivityDate.name), masterDf.col(CDLUpdatedTime.name), lit(null).cast(LongType)
      ).as(LastActivityDate.name)): _*)
  }
}
