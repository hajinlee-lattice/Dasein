package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.InterfaceName.{CDLUpdatedTime, LastActivityDate}
import com.latticeengines.domain.exposed.spark.cdl.MergeCuratedAttributesConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.MergeUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Merge curated attributes from multiple sources and do some consolidation
  */
class MergeCuratedAttributes extends AbstractSparkJob[MergeCuratedAttributesConfig] {
  override def runJob(spark: SparkSession, lattice: LatticeContext[MergeCuratedAttributesConfig]): Unit = {
    val config: MergeCuratedAttributesConfig = lattice.config
    val masterTableIdx = Option(config.masterTableIdx)
    val lastActivityDateIdx = Option(config.lastActivityDateInputIdx)
    val attrsToMerge = config.attrsToMerge.asScala.mapValues(_.asScala)

    // calculation
    val optLastActivityDf = if (lastActivityDateIdx.isDefined) {
      val lastActivityDf = lattice.input(config.lastActivityDateInputIdx)
      Some(masterTableIdx
        .map(lattice.input(_))
        .map(mergeLastActivityDate(lastActivityDf, _, config.joinKey))
        .getOrElse(lastActivityDf.select(config.joinKey, LastActivityDate.name)))
    } else {
      None: Option[DataFrame]
    }

    val mdf: DataFrame = if (attrsToMerge.isEmpty) {
      // must have val
      optLastActivityDf.get
    } else {
      val mergeFn = (accDf: DataFrame, args: (Integer, mutable.Buffer[String])) => {
        MergeUtils.merge2(
          accDf, lattice.input(args._1).select(config.joinKey, args._2: _*),
          Seq(config.joinKey), Set(), overwriteByNull = false)
      }
      optLastActivityDf match {
        case Some(df) => attrsToMerge.foldLeft(df)(mergeFn(_, _))
        case _ =>
          val df = lattice.input(attrsToMerge.head._1).select(config.joinKey, attrsToMerge.head._2: _*)
          attrsToMerge.tail.foldLeft(df)(mergeFn(_, _))
      }
    }

    lattice.output = mdf :: Nil
  }

  private def mergeLastActivityDate(lastActivityDf: DataFrame, accDf: DataFrame, joinKey: String): DataFrame = {
    accDf.join(lastActivityDf, Seq(joinKey), "left")
      .select(accDf.col(joinKey), coalesce(
        // pick last activity date first, then last update time
        lastActivityDf.col(LastActivityDate.name), accDf.col(CDLUpdatedTime.name), lit(null).cast(LongType)
      ).as(LastActivityDate.name))
  }
}
