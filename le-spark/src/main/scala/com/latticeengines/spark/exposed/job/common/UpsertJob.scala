package com.latticeengines.spark.exposed.job.common

import com.latticeengines.domain.exposed.spark.common.UpsertConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.MergeUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}

import scala.collection.JavaConverters._

class UpsertJob extends AbstractSparkJob[UpsertConfig] {
  
  private val systemColumn = "__system__"

  override def runJob(spark: SparkSession, lattice: LatticeContext[UpsertConfig]): Unit = {
    val config: UpsertConfig = lattice.config

    if (lattice.input.length == 1) {
      lattice.output = 
        if (config.getSystems == null || config.isSystemBatch) {
            lattice.input
          } else {
            addSystemPrefix(lattice.input.head, config.getSystems().get(0), Seq(config.getJoinKey)) :: Nil
          }
    } else {
      val switchSide = config.getSwitchSides != null && config.getSwitchSides
      val lhsDf = if (switchSide) lattice.input(1) else lattice.input.head
      val rhsDf = if (switchSide) lattice.input.head else lattice.input(1)
      val systems = if (config.getSystems == null) List() else config.getSystems.asScala.toList 
      
      val joinKey = config.getJoinKey
      val colsFromLhs: Set[String] = if (config.getColsFromLhs == null) Set() else config.getColsFromLhs.asScala.toSet
      val overwriteByNull: Boolean =
        if (config.getNotOverwriteByNull == null) true else !config.getNotOverwriteByNull.booleanValue()
        
      if (systems.size == 0) {
        val merged = MergeUtils.merge2(lhsDf, rhsDf, Seq(joinKey), colsFromLhs, overwriteByNull = overwriteByNull)
        lattice.output = merged :: Nil
      } else {
         val merged = upsertSystemBatch(lhsDf, rhsDf, Seq(joinKey), colsFromLhs, overwriteByNull, config.isSystemBatch, systems) 
        lattice.output = merged :: Nil
      }
    }
  }
  
  private def upsertSystemBatch(lhsDf: DataFrame, rhsDf: DataFrame, joinKeys: Seq[String], colsFromLhs: Set[String], //
             overwriteByNull: Boolean, systemBatch: Boolean, systems: List[String]): DataFrame = {
    var merged = 
      if (!systemBatch) addSystemPrefix(lhsDf, systems(0), joinKeys) else lhsDf
    for (i <- 1 to systems.length-1) {
      var system = systems(i)           
      var newRhsDf = filterBySystem(rhsDf, system)
      newRhsDf = addSystemPrefix(newRhsDf, system, joinKeys)
      var newColsFromLhs = colsFromLhs map (c => system + "__" + c)
      merged = MergeUtils.merge2(merged, newRhsDf, joinKeys, colsFromLhs.toSet, overwriteByNull = overwriteByNull)
   }
   merged
  }
  
  private def filterBySystem(df: DataFrame, system: String): DataFrame = {
      df.filter(col(systemColumn) === lit(system)).drop(systemColumn) 
  }
  
  private def addSystemPrefix(df: DataFrame, system: String, joinKeys: Seq[String]): DataFrame = {
    val newColumns = df.columns map (c => if (joinKeys.contains(c) || c.startsWith(system)) c else system + "__" + c)
    return df.toDF(newColumns:_*)
  }

  
}
