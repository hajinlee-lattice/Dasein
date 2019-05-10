package com.latticeengines.spark.exposed.job

import com.fasterxml.jackson.core.`type`.TypeReference
import com.latticeengines.common.exposed.util.JsonUtils
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit.StorageType
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit
import com.latticeengines.domain.exposed.spark.{SparkJobConfig, SparkJobResult}
import org.apache.commons.collections4.CollectionUtils
import org.apache.livy.scalaapi.ScalaJobContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._

abstract class AbstractSparkJob[C <: SparkJobConfig] extends (ScalaJobContext => String) {

  var serializedConfig: String = "{}"

  def configure(jobConfig: C): Unit = {
    serializedConfig = JsonUtils.serialize(jobConfig)
  }

  private def getConfig: C = {
    JsonUtils.deserializeByTypeRef(serializedConfig, new TypeReference[C] {})
  }

  override def apply(ctx: ScalaJobContext): String = {
    val (spark, latticeCtx) = initializeJob()
    runJob(spark, latticeCtx)
    val finalTargets = finalizeJob(spark, latticeCtx).asJava
    val result = new SparkJobResult()
    result.setTargets(finalTargets)
    result.setOutput(latticeCtx.outputStr)
    JsonUtils.serialize(result)
  }

  def initializeJob(): (SparkSession, LatticeContext[C]) = {
    val jobConfig: C = getConfig
    val spark = SparkSession.builder().appName(getClass.getSimpleName).getOrCreate()
    val checkpointDir =
      if (jobConfig.getWorkspace != null)
        jobConfig.getWorkspace + "/checkpoints"
      else
        "/spark-checkpoints"
    spark.sparkContext.setCheckpointDir(checkpointDir)
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
    val latticeCtx = new LatticeContext[C](stageInput, jobConfig, stageTargets)
    (spark, latticeCtx)
  }

  def loadHdfsUnit(spark: SparkSession, unit: HdfsDataUnit): DataFrame = {
    var path = unit.getPath
    if (!path.endsWith(".avro")) {
      if (path.endsWith("/")) {
        path += "*.avro"
      } else {
        path += "/*.avro"
      }
    }
    spark.read.format("avro").load("hdfs://" + path)
  }

  def finalizeJob(spark: SparkSession, latticeCtx: LatticeContext[C]): List[HdfsDataUnit] = {
    val targets: List[HdfsDataUnit] = latticeCtx.targets
    val output: List[DataFrame] = latticeCtx.output
    if (targets.length != output.length) {
      throw new IllegalArgumentException(s"${targets.length} targets are declared " //
        + s"but ${output.length} outputs are generated!")
    }
    targets.zip(output).map { t =>
      val tgt = t._1
      val df = t._2
      val path = tgt.getPath
      df.write.format("avro").save(path)
      val df2 = spark.read.format("avro").load(path)
      tgt.setCount(df2.count())
      tgt
    }
  }

  def runJob(spark: SparkSession, lattice: LatticeContext[C]): Unit

}
