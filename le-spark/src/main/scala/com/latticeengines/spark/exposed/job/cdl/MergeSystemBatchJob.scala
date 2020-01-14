package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.spark.cdl.MergeSystemBatchConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.MergeUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col}

import scala.collection.JavaConverters._

class MergeSystemBatchJob extends AbstractSparkJob[MergeSystemBatchConfig] {
  private val customerAccountIdField = InterfaceName.CustomerAccountId.name
  private val customerContactIdField = InterfaceName.CustomerContactId.name
  
  override def runJob(spark: SparkSession, lattice: LatticeContext[MergeSystemBatchConfig]): Unit = {
    val config: MergeSystemBatchConfig = lattice.config
    val joinKey = config.getJoinKey
    val systems = 
      if (config.getSystems == null) getSystems(lattice.input.head.columns, joinKey) else config.getSystems.asScala.toList 
      
      val overwriteByNull: Boolean =
        if (config.getNotOverwriteByNull == null) true else !config.getNotOverwriteByNull.booleanValue()
      var lhsDf = selectSystemBatch(lattice.input.head, systems(0), joinKey, config.isKeepPrefix)
      if (systems.length == 1) {
          lattice.output = lhsDf :: Nil
              
      } else {
        for ( i <- 1 until systems.length) {
          var rhsDf = selectSystemBatch(lattice.input.head, systems(i), joinKey, config.isKeepPrefix)
          lhsDf = MergeUtils.merge2(lhsDf, rhsDf, Seq(joinKey), Set(), overwriteByNull = overwriteByNull) 
        }
        lattice.output = lhsDf :: Nil
      }
  }

  private def selectSystemBatch(df: DataFrame, system: String, joinKey: String, keepPrefix: Boolean): DataFrame = {
      var fields = scala.collection.mutable.ListBuffer[String]()
      fields += joinKey
      df.columns.foreach { c =>
        if (c.startsWith(system + "__")) {
          fields += c
        }
      }
      var newDf = df.select(fields map col: _*)
      newDf = removeSystemPrefix(newDf, system, joinKey, keepPrefix)
      newDf.na.drop(2)
  }
  
  private def removeSystemPrefix(df: DataFrame, system: String, joinKey: String, keepPrefix: Boolean): DataFrame = {
    if (keepPrefix) {
      df
    } else {
      val newColumns = 
        df.columns map (c => if (c == joinKey) c else c.stripPrefix(system + "__"))
      return df.toDF(newColumns:_*)
    }
  }
  
  private def getSystems(columns: Array[String], joinKey: String): List[String] = {
    var result = scala.collection.mutable.ListBuffer[String]()
    var primarySystems = scala.collection.mutable.Set[String]()
    var secondarySystems = scala.collection.mutable.Set[String]()
    columns.foreach { c =>
      if (c != joinKey) { 
        val i = c.indexOf("__")
        val system = c.substring(0, i)
        val field = c.substring(i+2)
        if (field == customerAccountIdField || field == customerContactIdField) {
          primarySystems += system
        } else {
          secondarySystems += system
        }
      }
    }
    result ++= (secondarySystems diff primarySystems)
    result ++= primarySystems
    result.toList
  }
}
