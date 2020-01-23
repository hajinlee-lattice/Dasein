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
    
    if (lattice.input.length == 1) {
      lattice.output = 
        if (!config.isAddInputSystemBatch) {
            lattice.input
          } else {
            addTemplatePrefixForInput(lattice.input.head, Seq(config.getJoinKey)) :: Nil
          }
    } else {
      val switchSide = config.getSwitchSides != null && config.getSwitchSides
      val lhsDf = if (switchSide) lattice.input(1) else lattice.input.head
      val rhsDf = if (switchSide) lattice.input.head else lattice.input(1)
      
      val joinKey = config.getJoinKey
      val colsFromLhs: Set[String] = if (config.getColsFromLhs == null) Set() else config.getColsFromLhs.asScala.toSet
      val overwriteByNull: Boolean =
        if (config.getNotOverwriteByNull == null) true else !config.getNotOverwriteByNull.booleanValue()
        
      if (!config.isAddInputSystemBatch) {
        val merged = MergeUtils.merge2(lhsDf, rhsDf, Seq(joinKey), colsFromLhs, overwriteByNull = overwriteByNull)
        lattice.output = merged :: Nil
      } else {
         var templates = rhsDf.select(templateColumn).as[String].collect.toSet.toList
         val merged = upsertSystemBatch(lhsDf, rhsDf, Seq(joinKey), colsFromLhs, overwriteByNull, config.getBatchTemplateName, templates) 
        lattice.output = merged :: Nil
      }
    }
  }
  
  private def upsertSystemBatch(lhsDf: DataFrame, rhsDf: DataFrame, joinKeys: Seq[String], colsFromLhs: Set[String], //
             overwriteByNull: Boolean, templateName: String, templates: List[String]): DataFrame = {
    var merged = 
      if (templateName != null) addTemplatePrefix(lhsDf, templateName, joinKeys) else lhsDf
    for (i <- 0 to templates.length-1) {
      var template = templates(i)           
      var newRhsDf = filterByTemplate(rhsDf, template)
      newRhsDf = addTemplatePrefix(newRhsDf, template, joinKeys)
      var newColsFromLhs = colsFromLhs map (c => template + "__" + c)
      merged = MergeUtils.merge2(merged, newRhsDf, joinKeys, colsFromLhs.toSet, overwriteByNull = overwriteByNull)
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
