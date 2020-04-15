package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.spark.cdl.SplitSystemBatchStoreConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

class SplitSystemBatchStoreJob extends AbstractSparkJob[SplitSystemBatchStoreConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[SplitSystemBatchStoreConfig]): Unit = {
    val config: SplitSystemBatchStoreConfig = lattice.config
    val input: DataFrame = lattice.input.head
    val columns: Array[String] = lattice.input.head.columns
    val templateColMap = scanColumns(columns)

    if (config.getTemplates == null) throw new UnsupportedOperationException(s"No template specified")

    val templates = config.getTemplates.asScala.toList.sorted
    templates.foreach { template: String => {
      if (templateColMap.keySet.contains(template) == false) {
        throw new UnsupportedOperationException(s"template specified doesn't exist in the data")
      }
    }
    }

    println("----- BEGIN SCRIPT OUTPUT -----")
    println(s"templates under operation are: $templates")
    println("----- END SCRIPT OUTPUT -----")

    val discardFieldsList: List[String] = if (config.getDiscardFields == null) null else config.getDiscardFields.asScala.toList
    // base on the templates, generate # of templates dataset as output
    var output: Seq[DataFrame] = Seq()
    templates.foreach { template: String => {
        val colList = templateColMap.apply(template)
        val df: DataFrame = if (discardFieldsList == null) input.select(colList map col: _*) else input.select(colList map col: _*).drop(discardFieldsList: _*)
        // stripe out the prefix in the columns
        val newColumns =
          df.columns map (c => c.stripPrefix(template + "__"))
        output :+= df.toDF(newColumns: _*)
      }
    }

    lattice.output = output.toList
  }

  private def scanColumns(columns: Array[String]): scala.collection.mutable.Map[String, scala.collection.mutable.ListBuffer[String]] = {
    val templateColMap = scala.collection.mutable.Map[String, scala.collection.mutable.ListBuffer[String]]()

    // first scan to find columns with template prefix and populate the map
    columns.foreach { c =>
      val i = c.indexOf("__")
      if (i != -1) {
        val template = c.substring(0, i)
        val list = templateColMap.getOrElse(template, scala.collection.mutable.ListBuffer[String]())
        list += c
        templateColMap.put(template, list)
      }
    }
    // second scan to add those common columns for all templates
    columns.foreach { c =>
      val i = c.indexOf("__")
      if (i == -1) {
        templateColMap.foreach { case (k, v) => v += c }
      }
    }

    templateColMap
  }
}
