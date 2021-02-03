package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.common.exposed.util.CipherUtils
import com.latticeengines.domain.exposed.cdl.activitydata.ActivityAlert
import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.metadata.datastore.{HdfsDataUnit, S3DataUnit}
import com.latticeengines.domain.exposed.query.BusinessEntity
import com.latticeengines.domain.exposed.spark.cdl.PublishActivityAlertsJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.JavaConverters.mapAsScalaMapConverter

class PublishActivityAlertsJob extends AbstractSparkJob[PublishActivityAlertsJobConfig] {
  override def runJob(spark: SparkSession, lattice: LatticeContext[PublishActivityAlertsJobConfig]): Unit = {
    val config: PublishActivityAlertsJobConfig = lattice.config
    val alertsDf = if (config.getTableToPublish.isInstanceOf[HdfsDataUnit]) loadHdfsUnit(spark, config.getTableToPublish.asInstanceOf[HdfsDataUnit]) else loadS3Unit(spark, config.getTableToPublish.asInstanceOf[S3DataUnit])
    val alertNameToAlertCategory = typedLit(config.alertNameToAlertCategory.asScala)
    val exportDf = alertsDf //
      .withColumnRenamed(InterfaceName.AccountId.name(), ActivityAlert.ENTITY_ID_COL)
      .withColumn(ActivityAlert.ENTITY_TYPE_COL, lit(BusinessEntity.Account.name()))
      .withColumn(ActivityAlert.TENANT_ID_COL, lit(config.tenantId))
      .withColumn(ActivityAlert.CREATION_TIMESTAMP_COL, from_unixtime(col(InterfaceName.CreationTimestamp.name()) / 1000))
      .drop(InterfaceName.CreationTimestamp.name())
      .withColumn(ActivityAlert.VERSION_COL, lit(config.alertVersion))
      .withColumn(ActivityAlert.CATEGORY_COL, coalesce(alertNameToAlertCategory(alertsDf.col(InterfaceName.AlertName.name())), lit("")))
      .withColumnRenamed(InterfaceName.AlertName.name(), ActivityAlert.ALERT_NAME_COL)
      .withColumnRenamed(InterfaceName.AlertData.name(), ActivityAlert.ALERT_DATA_COL)

    val prop = new java.util.Properties
    prop.setProperty("driver", config.getDbDriver)
    prop.setProperty("user", config.getDbUser)
    prop.setProperty("password", CipherUtils.decrypt(config.getDbPassword, config.getDbRandomStr.substring(24), config.getDbRandomStr.substring(0, 24)))
    // default batchsize is 1000, scale up when dataset increases
    val batchSize = if (exportDf.count() / 100 < 1000) 1000 else exportDf.count() / 100
    prop.setProperty("batchsize", batchSize.toString)
    // cap repartition at 20 based on the experiments to avoid overloading DB
    val repartition: Int =
      if (exportDf.count() / 100000 == 0) {
        2
      } else if (exportDf.count() / 100000 < 20) {
        (exportDf.count() / 100000).toInt
      } else {
        20
      }
    val table = config.getDbTableName

    // write data from spark dataframe to database
    exportDf.repartition(repartition).write.mode(SaveMode.Append).jdbc(config.getDbUrl, table, prop)
  }
}
