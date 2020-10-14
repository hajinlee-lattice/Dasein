package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.common.exposed.util.CipherUtils
import com.latticeengines.domain.exposed.cdl.activitydata.ActivityAlert
import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit
import com.latticeengines.domain.exposed.query.BusinessEntity
import com.latticeengines.domain.exposed.spark.cdl.PublishActivityAlertsJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{coalesce, lit, typedLit}

import scala.collection.JavaConverters.mapAsScalaMapConverter

class PublishActivityAlertsJob extends AbstractSparkJob[PublishActivityAlertsJobConfig] {
  override def runJob(spark: SparkSession, lattice: LatticeContext[PublishActivityAlertsJobConfig]): Unit = {
    val config: PublishActivityAlertsJobConfig = lattice.config
    val alertsDf = loadHdfsUnit(spark, config.getTableToPublish.asInstanceOf[HdfsDataUnit])
    val alertNameToAlertCategory = typedLit(config.alertNameToAlertCategory.asScala)
    val exportDf = alertsDf //
      .withColumnRenamed(InterfaceName.AccountId.name(), ActivityAlert.ENTITY_ID_COL)
      .withColumn(ActivityAlert.ENTITY_TYPE_COL, lit(BusinessEntity.Account.name()))
      .withColumn(ActivityAlert.TENANT_ID_COL, lit(config.tenantId))
      .withColumnRenamed(InterfaceName.CreationTimestamp.name(), ActivityAlert.CREATION_TIMESTAMP_COL)
      .withColumnRenamed(InterfaceName.AlertName.name(), ActivityAlert.ALERT_NAME_COL)
      .withColumnRenamed(InterfaceName.AlertData.name(), ActivityAlert.ALERT_DATA_COL)
      .withColumn(ActivityAlert.VERSION_COL, lit(config.alertVersion))
      .withColumn(ActivityAlert.CATEGORY_COL, coalesce(alertNameToAlertCategory(alertsDf.col(InterfaceName.AlertName.name())), lit("")))

    val prop = new java.util.Properties
    prop.setProperty("driver", config.getDbDriver)
    prop.setProperty("user", config.getDbUser)
    prop.setProperty("password", CipherUtils.decrypt(config.getDbPassword))
    val table = config.getDbTableName

    //write data from spark dataframe to database
    exportDf.write.mode(SaveMode.Append).jdbc(config.getDbUrl, table, prop)
  }
}
