package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.spark.cdl.MergeImportsConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.MergeUtils
import org.apache.spark.sql.functions.{col, lit, when, count}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._

class MergeImportsJob extends AbstractSparkJob[MergeImportsConfig] {

  private val templateColumn = "__template__"
  private var hasSystem = false

  override def runJob(spark: SparkSession, lattice: LatticeContext[MergeImportsConfig]): Unit = {
    val config: MergeImportsConfig = lattice.config
    val inputDfs = lattice.input
    val joinKey = config.getJoinKey
    val srcId = config.getSrcId
    val templates: List[String] = if (config.getTemplates == null) List() else  config.getTemplates.asScala.toList
    hasSystem = config.isHasSystem
    val sortedInputs: Seq[DataFrame] = inputDfs.zip(lattice.inputCnts).sortBy(_._2).map(_._1)
    var processedInputs = sortedInputs map { src => processSrc(src, srcId, joinKey, config.isDedupSrc,
        config.getRenameSrcFields, config.getCloneSrcFields, hasSystem) }
    println("----- BEGIN SCRIPT OUTPUT -----")
    println(s"templates is: $templates")
    println("----- END SCRIPT OUTPUT -----")

    if (templates.nonEmpty) {
        processedInputs = processedInputs.zip(templates) map { e =>
          val df = e._1
          val template = e._2
          addTemplateColumn(df, template)
        }
    }
    val merged = processedInputs.zipWithIndex.reduce((l, r) => {
      val lhsDf = l._1
      val lhsIdx = l._2
      val rhsDf = r._1
      val rhsIdx = r._2
      val merge2 =
        if (joinKey != null && lhsDf.columns.contains(joinKey) && rhsDf.columns.contains(joinKey)) {
          var joinKeysForThisJoin = Seq(joinKey)
          if (hasSystem) {
              joinKeysForThisJoin = joinKeysForThisJoin :+ templateColumn
          }
          MergeUtils.merge2(lhsDf, rhsDf, joinKeysForThisJoin, Set(), overwriteByNull = false)
        } else {
          MergeUtils.concat2(lhsDf, rhsDf)
        }
      if (lhsIdx % 50 == 0 && lhsIdx > 0) {
        lhsDf.unpersist(blocking = false)
      }
      if (rhsIdx % 50 == 0 && rhsIdx > 0) {
        (merge2.persist(StorageLevel.DISK_ONLY).checkpoint(), rhsIdx)
      } else {
        (merge2, rhsIdx)
      }
    })._1

    val requiredCols: Map[String, String] =
      if (config.getRequiredColumns == null) Map() else config.getRequiredColumns.asScala.toMap
    val withRequiredCols =
      if (requiredCols.isEmpty) {
        merged
      } else {
        requiredCols.toList.foldLeft(merged)((df, p) => addAllNullsIfMissing(df, p._1, p._2))
      }

    val result =
      if (config.isAddTimestamps) {
        val currentTime = System.currentTimeMillis()
        addOrFill(
          addOrFill(withRequiredCols, InterfaceName.CDLCreatedTime.name(), currentTime),
          InterfaceName.CDLUpdatedTime.name(), currentTime)
      } else {
        withRequiredCols
      }

    // finish
    lattice.output = result :: Nil
  }

  private def processSrc(src: DataFrame, srcId: String, joinKey: String, deduplicate: Boolean,
      renameFlds: Array[Array[String]], cloneFlds: Array[Array[String]], hasSystem: Boolean): DataFrame = {
    val nullCount = if (joinKey != null) src.filter(col(joinKey).isNull).count else 0
    if (nullCount > 0) {
      throw new IllegalStateException(s"Import data has null value for key column=$joinKey!")
    }
    var fldUpd =  cloneSrcFlds(src, cloneFlds)
    fldUpd = renameSrcFlds(fldUpd, renameFlds)

    if (joinKey == null) {
      return fldUpd
    }

    val renamed =
      if (srcId != null && !srcId.equals(joinKey) && fldUpd.columns.contains(srcId)) {
        fldUpd.withColumnRenamed(srcId, joinKey)
      } else {
        fldUpd
      }

    var joinKeys = Seq(joinKey)
    if (hasSystem) {
      joinKeys = joinKeys :+ templateColumn
    }
    val dedup =
      if (deduplicate) {
        doDedupe(renamed, joinKeys)
      } else {
        renamed
      }

    dedup
  }
  private def doDedupe(df: DataFrame, joinKeys: Seq[String]): DataFrame = {

    var keyDf = df.select(joinKeys map col: _*)
    var dupKeyDf = keyDf.groupBy(joinKeys map col: _*)
      .agg(count("*").alias("_cnt"))
      .filter(col("_cnt") > 1)
      .drop(col("_cnt"))
    val dupDfCont = dupKeyDf.count()
    if (dupDfCont == 0) {
      return df
    }
    
    var joinedDf = MergeUtils.joinWithMarkers(df, dupKeyDf, joinKeys, "left")
    val (fromMarker, toMarker) = MergeUtils.getJoinMarkers()
    
    var dupDf = joinedDf.filter(col(toMarker).isNotNull)
    val mergeInGrp = new MergeInGroup(dupDf.schema, false)
    dupDf = dupDf.groupBy(joinKeys map col: _*).
      agg(mergeInGrp(dupDf.columns map col: _*).as("ColumnStruct")).
      select(col("ColumnStruct.*"))

    var distDf = joinedDf.filter(col(toMarker).isNull)
    
    distDf.union(dupDf).drop(Seq(fromMarker, toMarker): _*) 
  }

  private def renameSrcFlds(src: DataFrame, renameFlds: Array[Array[String]]): DataFrame = {
    if (renameFlds == null) {
      return src
    }

    var result = src
    for (fldPair <- renameFlds) {
      result =
        if (result.columns.contains(fldPair(0)) && !(result.columns.contains(fldPair(1)))) {
          result.withColumnRenamed(fldPair(0), fldPair(1))
        } else {
          result
        }
    }

    result
  }

  private def cloneSrcFlds(src: DataFrame, cloneFlds: Array[Array[String]]): DataFrame = {
    if (cloneFlds == null) {
      return src
    }

    var result = src
    for (fldPair <- cloneFlds) {
      result =
        if (result.columns.contains(fldPair(0))) {
          result.withColumn(fldPair(1), result.col(fldPair(0)))
        } else {
          result
        }
    }

    result
  }

  private def addOrFill(df: DataFrame, tsCol: String, ts: Long): DataFrame = {
    if (df.columns.contains(tsCol)) {
      df.withColumn(tsCol, when(col(tsCol).isNull, lit(ts)).otherwise(col(tsCol)))
    } else {
      df.withColumn(tsCol, lit(ts))
    }
  }

  private def addAllNullsIfMissing(df: DataFrame, requiredCol: String, colType: String): DataFrame = {
    if (df.columns.contains(requiredCol)) {
      df
    } else {
      df.withColumn(requiredCol, lit(null).cast(colType))
    }
  }

   private def addTemplateColumn(df: DataFrame, template: String): DataFrame = {
    if (df.columns.contains(templateColumn)) {
      df
    } else {
      df.withColumn(templateColumn, lit(template))
    }
  }

}
