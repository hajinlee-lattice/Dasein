package com.latticeengines.spark.exposed.job

import com.fasterxml.jackson.core.`type`.TypeReference
import com.latticeengines.common.exposed.util.JsonUtils
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit.StorageType
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit
import com.latticeengines.domain.exposed.spark.{SparkJobConfig, SparkJobResult}
import org.apache.commons.collections4.CollectionUtils
import org.apache.livy.scalaapi.ScalaJobContext
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

abstract class AbstractSparkJob[C <: SparkJobConfig] extends (ScalaJobContext => String) {

  var serializedConfig: String = "{}"
  val name:String

  def configure(jobConfig: C): Unit = {
    serializedConfig = JsonUtils.serialize(jobConfig)
  }

  def getConfig: C = {
    JsonUtils.deserializeByTypeRef(serializedConfig, new TypeReference[C] {})
  }

  override def apply(ctx: ScalaJobContext): String = {
    val (spark, stageInput, stageTargets) = initializeJob()
    val (outputData, outputStr) = runJob(spark, stageInput)
    val finalTargets = finalizeJob(stageTargets, outputData).asJava
    val result = new SparkJobResult()
    result.setTargets(finalTargets)
    result.setOutput(outputStr)
    JsonUtils.serialize(result)
  }

  def initializeJob(): (SparkSession, List[DataFrame], List[HdfsDataUnit]) = {
    val jobConfig: C = getConfig
    val spark = SparkSession.builder().appName(getClass.getSimpleName).getOrCreate()
    val stageInput: List[DataFrame] = if (CollectionUtils.isEmpty(jobConfig.getInput)) {
      Nil
    } else {
      jobConfig.getInput.asScala.map(dataUnit => {
        val storage = dataUnit.getStorageType
        storage match {
          case StorageType.Hdfs => loadHdfsUnit(spark, dataUnit.asInstanceOf[HdfsDataUnit])
          case _ => throw new UnsupportedOperationException(s"Unknown storage $storage")
        }
      }).toList
    }
    val stageTargets: List[HdfsDataUnit] = if (CollectionUtils.isEmpty(jobConfig.getTargets)) {
      Nil
    } else {
      jobConfig.getTargets.asScala.toList
    }
    (spark, stageInput, stageTargets)
  }

  def loadHdfsUnit(spark: SparkSession, unit: HdfsDataUnit): DataFrame = {
    val path = unit.getPath
    spark.read.format("com.databricks.spark.avro").load("hdfs://" + path)
  }

  def finalizeJob(targets: List[HdfsDataUnit], output: Map[Integer, DataFrame]): //
  List[HdfsDataUnit] = {
    targets.zipWithIndex map { t => {
      val tgt = t._1
      val idx = t._2
      if (output.contains(idx)) {
        val df = output.getOrElse(idx, null)
        val path = tgt.getPath
        df.write.format("com.databricks.spark.avro").save(path)
        tgt.setCount(df.count())
        tgt
      } else {
        throw new RuntimeException(s"Can not find $idx-th output dataframe: $output")
      }
    }
    }
  }

  def runJob(spark: SparkSession, stageInput: List[DataFrame]): (Map[Integer, DataFrame], String)

}
