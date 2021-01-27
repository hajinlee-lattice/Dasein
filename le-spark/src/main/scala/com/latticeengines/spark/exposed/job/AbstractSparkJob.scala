package com.latticeengines.spark.exposed.job

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import com.latticeengines.common.exposed.util.JsonUtils
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit.StorageType
import com.latticeengines.domain.exposed.metadata.datastore.{HdfsDataUnit, S3DataUnit}
import com.latticeengines.domain.exposed.spark.{SparkJobConfig, SparkJobResult}
import org.apache.commons.collections4.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.livy.scalaapi.ScalaJobContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.io.StringWriter
import scala.collection.JavaConverters._

abstract class AbstractSparkJob[C <: SparkJobConfig] extends (ScalaJobContext => String) {

  var serializedConfig: String = "{}"
  var applicationId: String = ""

  def serializeJson(obj: Any): String = {
    val mapper = getObjectMapper()
    val stringWriter: StringWriter = new StringWriter
    mapper.writeValue(stringWriter, obj)
    stringWriter.toString
  }

  private def getObjectMapper(): ObjectMapper = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper
  }

  def configure(jobConfig: C): Unit = {
    serializedConfig = JsonUtils.serialize(jobConfig)
  }

  private def getConfig: C = {
    JsonUtils.deserializeByTypeRef(serializedConfig, new TypeReference[C] {})
  }

  override def apply(ctx: ScalaJobContext): String = {
    applicationId = ctx.sc.applicationId
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
      List()
    } else {
      jobConfig.getInput.asScala.map(dataUnit => {
        if (dataUnit == null) {
          spark.emptyDataFrame
        } else {
          val storage = dataUnit.getStorageType
          storage match {
            case StorageType.Hdfs => loadHdfsUnit(spark, dataUnit.asInstanceOf[HdfsDataUnit])
            case StorageType.S3 => loadS3Unit(spark, dataUnit.asInstanceOf[S3DataUnit])
            case _ => throw new UnsupportedOperationException(s"Unknown storage $storage")
          }
        }
      }).toList
    }
    val stageInputCnts: List[Long] = if (CollectionUtils.isEmpty(jobConfig.getInput)) {
      List()
    } else {
      jobConfig.getInput.asScala.map(dataUnit => {
        val cnt: Long = if (dataUnit == null) {
          -1
        } else {
          if (dataUnit.getCount == null) -1 else dataUnit.getCount
        }
        cnt
      }).toList
    }
    val stageTargets: List[HdfsDataUnit] = if (CollectionUtils.isEmpty(jobConfig.getTargets)) {
      List()
    } else {
      jobConfig.getTargets.asScala.toList
    }
    val latticeCtx = new LatticeContext[C](stageInput, stageInputCnts, jobConfig, stageTargets)
    (spark, latticeCtx)
  }

  def getFilePath(suffix: String, filePrefix: String): String = {
    if (StringUtils.isNotEmpty(filePrefix)) {
      filePrefix + "*" + suffix
    } else {
      "*" + suffix
    }
  }

  def loadHdfsUnit(spark: SparkSession, unit: HdfsDataUnit): DataFrame = {
    var path = unit.getPath
    val fmt: String = if (unit.getDataFormat != null) unit.getDataFormat.name.toLowerCase else "avro"
    val partitionKeys = if (unit.getPartitionKeys == null) List() else unit.getPartitionKeys.asScala.toList
    if (partitionKeys.isEmpty) {
      val suffix = "." + fmt
      if (!path.endsWith(suffix)) {
        if (path.endsWith("/")) {
          path += getFilePath(suffix, unit.getFilePrefix)
        } else {
          path += "/" + getFilePath(suffix, unit.getFilePrefix)
        }
      }
    }
    if (fmt.equals("csv")) {
      spark.read.format(fmt) //
              .option("header", value = true) //
              .option("multiLine", value = true) // should avoid reading csv, because multiLine is purely on driver
              .option("quote", "\"") //
              .option("escape", "\"") //
              .load("hdfs://" + path)
    } else {
      spark.read.format(fmt).load("hdfs://" + path)
    }
  }

  def loadS3Unit(spark: SparkSession, unit: S3DataUnit): DataFrame = {
    val fmt: String = if (unit.getDataFormat != null) unit.getDataFormat.name.toLowerCase else "avro"
    if (fmt.equals("csv")) {
      spark.read.format(fmt) //
        .option("header", value = true) //
        .option("multiLine", value = true) // should avoid reading csv, because multiLine is purely on driver
        .option("quote", "\"") //
        .option("escape", "\"") //
        .load(s"s3a://${unit.getBucket}/${unit.getPrefix}")
    } else {
      spark.read.format(fmt).load(s"s3a://${unit.getBucket}/${unit.getPrefix}")
    }
  }

  def finalizeJob(spark: SparkSession, latticeCtx: LatticeContext[C]): List[HdfsDataUnit] = {
    val targets: List[HdfsDataUnit] = latticeCtx.targets
    val output: List[DataFrame] = latticeCtx.output

    val results = finalizeJob(spark, targets, output)
    latticeCtx.orphanViews map spark.catalog.dropTempView
    results
  }

  def finalizeJob(spark: SparkSession, targets: List[HdfsDataUnit], output: List[DataFrame]): List[HdfsDataUnit] = {
    if (targets.length != output.length) {
      throw new IllegalArgumentException(s"${targets.length} targets are declared " //
        + s"but ${output.length} outputs are generated!")
    }
    val jobConfig: C = getConfig
    val results = targets.zip(output).par.map { t =>
      val tgt = t._1
      var df = t._2
      val path = tgt.getPath
      if (jobConfig.numPartitionLimit != null) {
        df = coalesceTgt(df, jobConfig.numPartitionLimit)
      }
      if (tgt.isCoalesce) {
        df = df.coalesce(1)
      }
      val fmt = if (tgt.getDataFormat != null) tgt.getDataFormat.name.toLowerCase else "avro"
      if (fmt.equals("csv")) {
        df = df.persist(StorageLevel.DISK_ONLY)
      }
      val partitionKeys = if (tgt.getPartitionKeys == null) List() else tgt.getPartitionKeys.asScala.toList
      if (partitionKeys.isEmpty) {
        df.write.format(fmt).save(path)
      } else {
        df.write.partitionBy(partitionKeys: _*).format(fmt).save(path)
      }
      if (fmt.equals("csv")) {
        tgt.setCount(df.count())
      } else {
        val df2 = spark.read.format(fmt).load(path)
        tgt.setCount(df2.count())
      }
      tgt
    }.toList
    results
  }

  def logSpark(message: String): Unit = {
    println(s"[$applicationId]: $message")
  }

  def logDataFrame(dfName: String, df: DataFrame, sortKey: String, selection: Seq[String], limit: Int) = {
    logSpark("==========" + dfName + "==========")
    logSpark(selection mkString ",")
    df.orderBy(sortKey).select(selection map col: _*).limit(limit).collect().foreach(r => logSpark(r.toString))
    logSpark("==========" + dfName + "==========")
  }

  def setPartitionTargets(index: Int, list: Seq[String], lattice: LatticeContext[C]): Unit = {
    if (index >= 0 && index < lattice.targets.size) {
      lattice.targets(index).setPartitionKeys(list.asJava);
    } else {
      throw new RuntimeException(s"There's no Target $index")
    }
  }

  def coalesceTgt(df: DataFrame, numPartitionLimit: Int): DataFrame = {
    if (df.rdd.getNumPartitions >= numPartitionLimit) {
      df.coalesce((numPartitionLimit + 1) / 2)
    } else {
      df
    }
  }

  def runJob(spark: SparkSession, lattice: LatticeContext[C]): Unit

}
