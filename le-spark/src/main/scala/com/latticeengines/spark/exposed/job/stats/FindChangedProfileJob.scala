package com.latticeengines.spark.exposed.job.stats

import com.latticeengines.domain.exposed.datacloud.DataCloudConstants
import com.latticeengines.domain.exposed.spark.stats.FindChangedProfileConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

class FindChangedProfileJob extends AbstractSparkJob[FindChangedProfileConfig] {

  private val AttrName = DataCloudConstants.PROFILE_ATTR_ATTRNAME
  private val BktAlgo = DataCloudConstants.PROFILE_ATTR_BKTALGO

  override def runJob(spark: SparkSession, lattice: LatticeContext[FindChangedProfileConfig]): Unit = {
    val oldProfile = lattice.input.head.select(col(AttrName), col(BktAlgo))
    val newProfile = lattice.input(1).select(col(AttrName), col(BktAlgo))
    val result = newProfile.alias("lhs").join(oldProfile.alias("rhs"), Seq(AttrName), joinType = "left")
      .filter((col(s"lhs.$BktAlgo").isNull && col(s"rhs.$BktAlgo").isNotNull) ||
        (col(s"lhs.$BktAlgo").isNotNull && col(s"rhs.$BktAlgo").isNull) ||
        col(s"lhs.$BktAlgo") =!= col(s"rhs.$BktAlgo"))
      .select(s"lhs.$AttrName")
    val attrs: Seq[String] = result.collect().map(r => r.getString(0))
    lattice.outputStr = serializeJson(attrs)
  }

}
