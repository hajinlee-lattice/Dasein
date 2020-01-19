package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.spark.cdl.MergeSystemBatchConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.MergeUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col}
import org.apache.commons.collections4.CollectionUtils
import scala.collection.JavaConverters._
import util.control.Breaks._

class MergeSystemBatchJob extends AbstractSparkJob[MergeSystemBatchConfig] {
  private val customerAccountIdField = InterfaceName.CustomerAccountId.name
  private val customerContactIdField = InterfaceName.CustomerContactId.name
  
  override def runJob(spark: SparkSession, lattice: LatticeContext[MergeSystemBatchConfig]): Unit = {
    val config: MergeSystemBatchConfig = lattice.config
    val joinKey = config.getJoinKey
    val templates = 
      if (CollectionUtils.isEmpty(config.getTemplates)) getTemplates(lattice.input.head.columns, joinKey) else config.getTemplates.asScala.toList 
      
      val overwriteByNull: Boolean =
        if (config.getNotOverwriteByNull == null) true else !config.getNotOverwriteByNull.booleanValue()
      var lhsDf = selectSystemBatch(lattice.input.head, templates(0), joinKey, config.isKeepPrefix)
      if (templates.length == 1) {
          lattice.output = lhsDf :: Nil
              
      } else {
        for ( i <- 1 until templates.length) {
          breakable {
            var rhsDf = selectSystemBatch(lattice.input.head, templates(i), joinKey, config.isKeepPrefix)
            if (rhsDf.count() == 0) break
            lhsDf = MergeUtils.merge2(lhsDf, rhsDf, Seq(joinKey), Set(), overwriteByNull = overwriteByNull) 
          }
        }
        lattice.output = lhsDf :: Nil
      }
  }

  private def selectSystemBatch(df: DataFrame, template: String, joinKey: String, keepPrefix: Boolean): DataFrame = {
      var fields = scala.collection.mutable.ListBuffer[String]()
      fields += joinKey
      df.columns.foreach { c =>
        if (c.startsWith(template + "__")) {
          fields += c
        }
      }
      var newDf = df.select(fields map col: _*)
      newDf = removeTemplatePrefix(newDf, template, joinKey, keepPrefix)
      newDf.na.drop(2)
  }
  
  private def removeTemplatePrefix(df: DataFrame, template: String, joinKey: String, keepPrefix: Boolean): DataFrame = {
    if (keepPrefix) {
      df
    } else {
      val newColumns = 
        df.columns map (c => if (c == joinKey) c else c.stripPrefix(template + "__"))
      return df.toDF(newColumns:_*)
    }
  }
  
  private def getTemplates(columns: Array[String], joinKey: String): List[String] = {
    var result = scala.collection.mutable.ListBuffer[String]()
    var primaryTemplates = scala.collection.mutable.Set[String]()
    var secondaryTemplates = scala.collection.mutable.Set[String]()
    columns.foreach { c =>
      if (c != joinKey) { 
        val i = c.indexOf("__")
        val template = c.substring(0, i)
        val field = c.substring(i+2)
        if (field == customerAccountIdField || field == customerContactIdField) {
          primaryTemplates += template
        } else {
          secondaryTemplates += template
        }
      }
    }
    result ++= (secondaryTemplates diff primaryTemplates)
    result ++= primaryTemplates
    result.toList
  }
}
