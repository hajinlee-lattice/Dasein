package com.latticeengines.spark.exposed.job.stats

import com.latticeengines.common.exposed.util.JsonUtils
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants
import com.latticeengines.domain.exposed.datacloud.dataflow.{BucketAlgorithm, CategoricalBucket, DiscreteBucket}
import com.latticeengines.domain.exposed.spark.common.ChangeListConstants._
import com.latticeengines.domain.exposed.spark.stats.UpdateProfileConfig
import com.latticeengines.spark.aggregation.UpdateProfileAggregation
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.{ChangeListUtils, MergeUtils, ProfileUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import scala.collection.JavaConverters._

class UpdateProfileJob extends AbstractSparkJob[UpdateProfileConfig] {

  private val AttrName = DataCloudConstants.PROFILE_ATTR_ATTRNAME
  private val BktAlgo = DataCloudConstants.PROFILE_ATTR_BKTALGO

  override def runJob(spark: SparkSession, lattice: LatticeContext[UpdateProfileConfig]): Unit = {
    val config: UpdateProfileConfig = lattice.config
    val includeAttrs: Set[String] = config.getIncludeAttrs.asScala.toSet
    val bCastIncAttrs = spark.sparkContext.broadcast(includeAttrs)
    val attrNameIdx = lattice.input(1).columns.indexOf(AttrName)
    val profile = lattice.input(1).filter(row => bCastIncAttrs.value.contains(row.getString(attrNameIdx))).cache()

    val valMap: Map[String, Seq[Any]] = profile.select(AttrName, BktAlgo).collect().map(row => {
      val attrName = row.getString(0)
      val bktAlgoStr = row.getString(1)
      val values = if (bktAlgoStr == null) {
        None
      } else {
        val bktAlgo = JsonUtils.deserialize(bktAlgoStr, classOf[BucketAlgorithm])
        bktAlgo match {
          case catBkt: CategoricalBucket => Some(catBkt.getCategories.asScala)
          case disBkt: DiscreteBucket => Some(disBkt.getValues.asScala)
          case _ => None
        }
      }
      (attrName, values)
    }).filter(t => t._2.isDefined).map(t => (t._1, t._2.get)).toMap
    val bCastValMap = spark.sparkContext.broadcast(valMap)

    val aggr = new UpdateProfileAggregation(bCastValMap, config.getMaxCat, config.getMaxCatLength, config.getMaxDiscrete)
    val colIdIdx = lattice.input.head.columns.indexOf(ColumnId)
    val changelist = lattice.input.head //
      .filter(row => bCastIncAttrs.value.contains(row.getString(colIdIdx)) //
        && bCastValMap.value.contains(row.getString(colIdIdx)) //
        && ChangeListUtils.getToValue(row) != null)
    val newBktAlgo = changelist.groupBy(ColumnId, DataType).agg(aggr( //
      col(DataType), //
      col(ToString), col(ToInteger), col(ToLong), col(ToFloat), col(ToDouble), //
      col(ColumnId) //
    ).as(BktAlgo)).withColumnRenamed(ColumnId, AttrName)

    val merged = MergeUtils.merge2(profile, newBktAlgo, Seq(AttrName), Set(), overwriteByNull = true)

    // numerical attrs exceeding max discrete values need to be re-profiled
    val output = merged //
      .filter(col(DataType).isNull || col(DataType) === "String" || col(BktAlgo).isNotNull) //
      .drop(DataType) //
      .select(ProfileUtils.colsInOrder(): _*)
    lattice.output = List(output)
  }

}
