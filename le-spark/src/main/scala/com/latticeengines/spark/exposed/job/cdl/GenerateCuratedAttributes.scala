package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.metadata.InterfaceName.{CDLUpdatedTime, LastActivityDate}
import com.latticeengines.domain.exposed.spark.cdl.GenerateCuratedAttributesConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.MergeUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
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
    val lastActivityDateIdx = Option(config.lastActivityDateInputIdx)
    val attrsToMerge = config.attrsToMerge.asScala.mapValues(_.asScala)
    val specialJoinKeys = config.joinKeys.asScala
    val templateSystemMap = config.templateSystemMap.asScala
    val templateTypeMap = config.templateTypeMap.asScala
    val templateSysTypeMap = config.templateSystemTypeMap.asScala
    val masterDf = lattice.input(config.masterTableIdx)

    // calculation
    val optLastActivityDf = if (lastActivityDateIdx.isDefined) {
      val lastActivityDf = lattice.input(config.lastActivityDateInputIdx)
      Some(mergeLastActivityDate(lastActivityDf, masterDf, config.joinKey, config.columnsToIncludeFromMaster.asScala.toList))
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
          accDf, mergeAttr(df, args._2, templateSystemMap, templateTypeMap, templateSysTypeMap).select(config.joinKey, toTargetAttrs(args._2): _*),
          Seq(config.joinKey), Set(), overwriteByNull = false)
      }
      optLastActivityDf match {
        case Some(df) => attrsToMerge.foldLeft(df)(mergeFn(_, _))
        case _ =>
          val inputIdx = attrsToMerge.head._1
          val srcJoinKey = specialJoinKeys.getOrElse(inputIdx, config.joinKey)
          val df = mergeAttr(modifyJoinKey(lattice.input(inputIdx), config.joinKey, srcJoinKey),
            attrsToMerge.head._2, templateSystemMap, templateTypeMap, templateSysTypeMap)
            .select(config.joinKey, toTargetAttrs(attrsToMerge.head._2): _*)
          attrsToMerge.tail.foldLeft(df)(mergeFn(_, _))
      }
    }

    lattice.output = filterOrphanRecords(mdf, lattice) :: Nil
  }

  private def filterOrphanRecords(df: DataFrame, lattice: LatticeContext[GenerateCuratedAttributesConfig]): DataFrame = {
    val config = lattice.config
    val masterDf = lattice.input(config.masterTableIdx)
    val masterJoinKey = config.joinKey
    val specialJoinKeys = config.joinKeys.asScala
    Option(config.parentMasterTableIdx).map { idx =>
      val parentJoinKey = specialJoinKeys(idx)
      val parentDf = lattice.input(idx)
      parentDf
        .select(parentJoinKey)
        .join(masterDf.select(parentJoinKey, masterJoinKey), parentJoinKey) // non-orphan contacts
        .drop(parentJoinKey)
        .join(df, Seq(masterJoinKey), "inner")
    } getOrElse (df)
  }

  // generate a seq of all target attributes
  private def toTargetAttrs(attrsToMerge: mutable.Map[String, String]): Seq[String] = {
    val expandAttrFn = (attrs: (String, String)) => {
      val srcAttr = attrs._1
      val tgtAttr = attrs._2
      if (InterfaceName.CDLCreatedTemplate.name().equals(srcAttr)) {
        // tgtAttr will be the prefix, create two attrs (EntityCreatedSource and EntityCreatedType)
        val createdSrcAttr = tgtAttr + InterfaceName.EntityCreatedSource.name
        val createdTypeAttr = tgtAttr + InterfaceName.EntityCreatedType.name
        Seq(createdSrcAttr, createdTypeAttr, InterfaceName.EntityCreatedSystemType.name)
      } else {
        Seq(tgtAttr)
      }
    }
    attrsToMerge.flatMap(expandAttrFn).toSeq
  }

  private def modifyJoinKey(df: DataFrame, tgtJoinKey: String, srcJoinKey: String): DataFrame = {
    val cols = df.columns.toSeq
    if (cols.contains(tgtJoinKey) || tgtJoinKey.equals(srcJoinKey)) {
      df
    } else {
      df.withColumnRenamed(srcJoinKey, tgtJoinKey)
    }
  }

  private def mergeAttr(df: DataFrame, attrs: mutable.Map[String, String], templateSystemMap: mutable.Map[String, String],
                        templateTypeMap: mutable.Map[String, String], templateSysTypeMap: mutable.Map[String, String]): DataFrame = {
    val mapTemplateToSystem = udf((s: String) => templateSystemMap.getOrElse(s, null))
    val mapTemplateToEntityType = udf((s: String) => templateTypeMap.getOrElse(s, null))
    val mapTemplateToSystemType = udf((s: String) => templateSysTypeMap.getOrElse(s, null))
    attrs.foldLeft(df)((accDf, args) => {
      val tgtAttr = args._2
      val srcAttr = args._1
      val cols = accDf.columns.toSeq
      if (cols.contains(tgtAttr)) {
        // TODO consider failing job
        accDf
      } else if (InterfaceName.CDLCreatedTemplate.name().equals(srcAttr) && tgtAttr != null) {
        // tgtAttr will be the prefix, create two attrs (EntityCreatedSource and EntityCreatedType)
        val createdSrcAttr = tgtAttr + InterfaceName.EntityCreatedSource.name
        val createdTypeAttr = tgtAttr + InterfaceName.EntityCreatedType.name
        val createdSystemTypeAttr = InterfaceName.EntityCreatedSystemType.name

        var createdDf = accDf
        createdDf = populateAttrFromSource(createdDf, createdSrcAttr, srcAttr, mapTemplateToSystem, cols)
        createdDf = populateAttrFromSource(createdDf, createdTypeAttr, srcAttr, mapTemplateToEntityType, cols)
        createdDf = populateAttrFromSource(createdDf, createdSystemTypeAttr, srcAttr, mapTemplateToSystemType, cols)
        createdDf
      } else if (!cols.contains(srcAttr)) {
        // TODO pass in tgt attribute type, use attr name to support time field for now
        if (tgtAttr.endsWith("Date") || tgtAttr.endsWith("Time")) {
          accDf.withColumn(tgtAttr, lit(null).cast(LongType))
        } else {
          accDf.withColumn(tgtAttr, lit(null).cast(StringType))
        }
      } else {
        accDf.withColumn(tgtAttr, accDf.col(srcAttr))
      }
    })
  }

  private def populateAttrFromSource(df: DataFrame, attr: String, srcAttr: String, mapper: UserDefinedFunction, cols: Seq[String]): DataFrame = {
    if (!cols.contains(attr)) {
      if (cols.contains(srcAttr)) {
        df.withColumn(attr, mapper(df.col(srcAttr)))
      } else {
        df.withColumn(attr, lit(null).cast(StringType))
      }
    } else {
      df
    }
  }

  private def mergeLastActivityDate(lastActivityDf: DataFrame, masterDf: DataFrame, joinKey: String, additionalColumns: List[String]): DataFrame = {
    val colsFromMaster = additionalColumns.intersect(masterDf.columns).diff(Seq(joinKey))
    // There was a corner case where joinKey is ContactId, but both sides have AccountId
    val rhs = lastActivityDf.drop(lastActivityDf.columns.intersect(colsFromMaster): _*)
    masterDf.alias("lhs").join(rhs.alias("rhs"), Seq(joinKey), "left")
      .withColumn(LastActivityDate.name, coalesce( //
        col(s"rhs.${LastActivityDate.name}"), //
        col(s"lhs.${CDLUpdatedTime.name}"), //
        lit(null).cast(LongType) //
      ))
      .select(col(s"lhs.$joinKey") :: col(s"${LastActivityDate.name}") :: colsFromMaster.map(col): _*)
  }
}
