package com.latticeengines.spark.exposed.job.common

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.spark.common.UpsertConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.MergeUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, when}

import scala.collection.JavaConverters._

class UpsertJob extends AbstractSparkJob[UpsertConfig] {
  
  private val templateColumn = "__template__"

  override def runJob(spark: SparkSession, lattice: LatticeContext[UpsertConfig]): Unit = {
    val spark = SparkSession.builder.getOrCreate    
    import spark.implicits._
    
    val config: UpsertConfig = lattice.config
    val discardAttrs: List[String] = if (config.getExcludeAttrs == null) List() else config.getExcludeAttrs.asScala.toList
    val eraseByNull = config.isEraseByNullEnabled != null && config.isEraseByNullEnabled

    if (lattice.input.length == 1) {
      var result =
        if (!config.isAddInputSystemBatch) {
            lattice.input.head
          } else {
            if (config.getBatchTemplateName == null) {
              addTemplatePrefixForInput(lattice.input.head, Seq(config.getJoinKey))
            } else {
              addTemplatePrefix(lattice.input.head, config.getBatchTemplateName,  Seq(config.getJoinKey))
            }
          }
      result = MergeUtils.dropEraseColumn(result, eraseByNull)
      if (discardAttrs.nonEmpty) {
        result = result.drop(discardAttrs: _*)
      }
      lattice.output = result :: Nil
    } else {
      val switchSide = config.getSwitchSides != null && config.getSwitchSides
      val lhsDf = if (switchSide) lattice.input(1) else lattice.input.head
      val rhsDf = if (switchSide) lattice.input.head else lattice.input(1)
      
      val joinKey = config.getJoinKey
      val colsFromLhs: Set[String] = if (config.getColsFromLhs == null) Set() else config.getColsFromLhs.asScala.toSet
      val overwriteByNull: Boolean =
        if (config.getNotOverwriteByNull == null) true else !config.getNotOverwriteByNull.booleanValue()

      var merged =
        if (!config.isAddInputSystemBatch) {
          MergeUtils.mergeWithEraseByNull(lhsDf, rhsDf, Seq(joinKey), colsFromLhs, overwriteByNull = overwriteByNull , eraseByNull = eraseByNull)
        } else {
          val templates = rhsDf.select(templateColumn).as[String].collect.toSet.toList
          upsertSystemBatch(lhsDf, rhsDf, Seq(joinKey), colsFromLhs, overwriteByNull, config.getBatchTemplateName, templates, eraseByNull)
        }
      if (discardAttrs.nonEmpty) {
        merged = merged.drop(discardAttrs: _*)
      }
      lattice.output = merged :: Nil
    }
  }

  private def upsertSystemBatch(origLhsDf: DataFrame, origRhsDf: DataFrame, joinKeys: Seq[String], colsFromLhs: Set[String], //
             overwriteByNull: Boolean, templateName: String, templates: List[String], eraseByNull: Boolean): DataFrame = {
    var lhsDf = origLhsDf.repartition(joinKeys map col: _*)
    var rhsDf = origRhsDf.repartition(joinKeys map col: _*)
    var merged = 
      if (templateName != null) addTemplatePrefix(lhsDf, templateName, joinKeys) else lhsDf
    for (i <- 0 to templates.length-1) {
      var template = templates(i)           
      var newRhsDf = filterByTemplate(rhsDf, template)
      newRhsDf = addTemplatePrefix(newRhsDf, template, joinKeys)
      var newColsFromLhs = colsFromLhs map (c => template + "__" + c)
      merged = MergeUtils.mergeWithEraseByNull(merged, newRhsDf, joinKeys, colsFromLhs.toSet, overwriteByNull = overwriteByNull, eraseByNull = eraseByNull)
   }
   merged
  }
  
  private def filterByTemplate(df: DataFrame, template: String): DataFrame = {
      df.filter(col(templateColumn) === lit(template)).drop(templateColumn) 
  }
  
  private def addTemplatePrefixForInput(df: DataFrame, joinKeys: Seq[String]): DataFrame = {
    val spark = SparkSession.builder.getOrCreate    
    import spark.implicits._
    var templates = df.select(templateColumn).as[String].collect.toSet.toList
    var merged = filterByTemplate(df, templates(0))
    merged = addTemplatePrefix(merged, templates(0), joinKeys)
    for (i <- 1 to templates.length-1) {
      var template = templates(i)           
      var newDf = filterByTemplate(df, template)
      newDf = addTemplatePrefix(newDf, template, joinKeys)
      merged = MergeUtils.merge2(merged, newDf, joinKeys, Set(), false)
    }
    merged
  }
  
  private def addTemplatePrefix(df: DataFrame, template: String, joinKeys: Seq[String]): DataFrame = {
    val newColumns = df.columns map (c => if (joinKeys.contains(c) || c.startsWith(template)) c else template + "__" + c)
    var newDf = df.toDF(newColumns:_*)
    newDf.withColumn(template + "__" + InterfaceName.CDLBatchSource.name, when(lit(true), 1))
  }
  
}
