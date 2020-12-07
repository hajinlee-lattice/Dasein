package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.datacloud.contactmaster.ContactMasterConstants;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit
import com.latticeengines.domain.exposed.spark.cdl.GenerateLiveRampLaunchArtifactsJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

class GenerateLiveRampLaunchArtifactsJob extends AbstractSparkJob[GenerateLiveRampLaunchArtifactsJobConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[GenerateLiveRampLaunchArtifactsJobConfig]): Unit = {
    val config: GenerateLiveRampLaunchArtifactsJobConfig = lattice.config
    val recordId = ContactMasterConstants.TPS_ATTR_RECORD_ID

    val addContactsDf = if (config.getAddContacts != null) loadHdfsUnit(spark, config.getAddContacts.asInstanceOf[HdfsDataUnit]) else spark.createDataFrame(spark.sparkContext.emptyRDD[Row], getSchema())
    val removeContactsDf = if (config.getRemoveContacts != null) loadHdfsUnit(spark, config.getRemoveContacts.asInstanceOf[HdfsDataUnit]) else spark.createDataFrame(spark.sparkContext.emptyRDD[Row], getSchema())
    var distinctAddContactsDf = addContactsDf.dropDuplicates(recordId)
    var distinctRemoveContactsDf = removeContactsDf.dropDuplicates(recordId)

    lattice.output = List(distinctAddContactsDf, distinctRemoveContactsDf)
  }

  def getSchema(): StructType = {
    StructType(
      StructField(ContactMasterConstants.TPS_ATTR_RECORD_ID, StringType, nullable = true) :: Nil)
  }

}
