package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.spark.cdl.MergeImportsConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.MergeUtils
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._

class MergeImportsJob extends AbstractSparkJob[MergeImportsConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[MergeImportsConfig]): Unit = {
    val config: MergeImportsConfig = lattice.config
    val inputDfs = lattice.input
    val joinKey = config.getJoinKey
    val srcId = config.getSrcId

    val processedInputs = inputDfs map { src => processSrc(src, srcId, joinKey, config.isDedupSrc,
        config.getRenameSrcFields, config.getCloneSrcFields) }

    val merged = processedInputs.zipWithIndex.reduce((l, r) => {
      val lhsDf = l._1
      val lhsIdx = l._2
      val rhsDf = r._1
      val rhsIdx = r._2
      val merge2 =
        if (joinKey != null && lhsDf.columns.contains(joinKey) && rhsDf.columns.contains(joinKey)) {
          val joinKeysForThisJoin = Seq(joinKey)
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
      renameFlds: Array[Array[String]], cloneFlds: Array[Array[String]]): DataFrame = {
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

    val dedup =
      if (deduplicate) {
        val mergeInGrp = new MergeInGroup(renamed.schema, false)
        renamed.groupBy(joinKey).
          agg(mergeInGrp(renamed.columns map col: _*).as("ColumnStruct")).
          select(col("ColumnStruct.*"))
      } else {
        renamed
      }

    dedup
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

}
