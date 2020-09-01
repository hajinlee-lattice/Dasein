package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName
import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit
import com.latticeengines.domain.exposed.query.BusinessEntity
import com.latticeengines.domain.exposed.spark.cdl.GenerateLaunchArtifactsJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.CountryCodeUtils
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

class GenerateLaunchArtifactsJob extends AbstractSparkJob[GenerateLaunchArtifactsJobConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[GenerateLaunchArtifactsJobConfig]): Unit = {
    val config: GenerateLaunchArtifactsJobConfig = lattice.config
    val mainEntity = config.getMainEntity
    val externalSystemName = config.getExternalSystemName
    val url = config.getManageDbUrl
    val user = config.getUser
    val password = config.getPassword
    val key = config.getEncryptionKey
    val salt = config.getSaltHint
    val accountId = InterfaceName.AccountId.name()
    val contactId = InterfaceName.ContactId.name()
    val country = InterfaceName.Country.name()
    val contactCountry = InterfaceName.ContactCountry.name()

    val accountsDf = loadHdfsUnit(spark, config.getAccountsData.asInstanceOf[HdfsDataUnit])
    val contactsDf = if (config.getContactsData != null) loadHdfsUnit(spark, config.getContactsData.asInstanceOf[HdfsDataUnit]) else spark.createDataFrame(spark.sparkContext.emptyRDD[Row], getSchema(BusinessEntity.Contact))
    val targetSegmentsContactsDF = if (config.getTargetSegmentsContactsData != null) loadHdfsUnit(spark, config.getTargetSegmentsContactsData.asInstanceOf[HdfsDataUnit]) else spark.createDataFrame(spark.sparkContext.emptyRDD[Row], getSchema(BusinessEntity.Contact))
    val positiveDeltaDf = if (config.getPositiveDelta != null) loadHdfsUnit(spark, config.getPositiveDelta.asInstanceOf[HdfsDataUnit]) else spark.createDataFrame(spark.sparkContext.emptyRDD[Row], getSchema(mainEntity))
    val negativeDeltaDf = if (config.getNegativeDelta != null) loadHdfsUnit(spark, config.getNegativeDelta.asInstanceOf[HdfsDataUnit]) else spark.createDataFrame(spark.sparkContext.emptyRDD[Row], getSchema(mainEntity))
    var distinctPositiveAccountsDf = positiveDeltaDf
    var distinctNegativeAccountsDf = negativeDeltaDf

    if (mainEntity == BusinessEntity.Contact) {
      distinctPositiveAccountsDf = positiveDeltaDf.select(positiveDeltaDf(accountId)).distinct()
      distinctNegativeAccountsDf = negativeDeltaDf.select(negativeDeltaDf(accountId)).distinct()
    }

    var addedAccountsData = accountsDf.join(distinctPositiveAccountsDf, Seq(accountId))
    val removedAccountsData = accountsDf.join(distinctNegativeAccountsDf, Seq(accountId), "right")
    var fullContactsData = targetSegmentsContactsDF.join(distinctPositiveAccountsDf, Seq(accountId), if (mainEntity == BusinessEntity.Contact && config.isIncludeAccountsWithoutContacts) "right" else "inner")

    if (mainEntity == BusinessEntity.Account && externalSystemName == CDLExternalSystemName.LinkedIn) {
      addedAccountsData = CountryCodeUtils.convert(addedAccountsData, country, country, url, user, password, key, salt)
    }
    lattice.output = List(addedAccountsData, removedAccountsData, fullContactsData)

    if (mainEntity == BusinessEntity.Contact) {
      var addedContactsData = contactsDf.drop(accountId).join(positiveDeltaDf, Seq(contactId), if (config.isIncludeAccountsWithoutContacts) "right" else "inner")
      var removedContactsData = contactsDf.drop(accountId).join(negativeDeltaDf, Seq(contactId), "right")

      if (CDLExternalSystemName.AD_PLATFORMS.contains(externalSystemName)) {
        addedContactsData = CountryCodeUtils.convert(addedContactsData, contactCountry, contactCountry, url, user, password, key, salt)
        fullContactsData = CountryCodeUtils.convert(fullContactsData, contactCountry, contactCountry, url, user, password, key, salt)
      }
      lattice.output = List(addedAccountsData, removedAccountsData, fullContactsData, addedContactsData, removedContactsData)
    }
  }

  def getSchema(entity: BusinessEntity): StructType = {
    if (entity == BusinessEntity.Account) {
      return StructType(
        StructField(InterfaceName.AccountId.name(), StringType, nullable = true) :: Nil)
    }
    StructType(
      StructField(InterfaceName.AccountId.name(), StringType, nullable = true) ::
        StructField(InterfaceName.ContactId.name(), StringType, nullable = true) :: Nil)
  }

}
