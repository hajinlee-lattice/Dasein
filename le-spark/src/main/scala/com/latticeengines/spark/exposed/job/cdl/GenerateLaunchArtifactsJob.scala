package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName
import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.query.BusinessEntity
import com.latticeengines.domain.exposed.spark.cdl.GenerateLaunchArtifactsJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.CountryCodeUtils
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ListBuffer, Set}
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
    val accountAttributes: Set[String] = config.getAccountAttributes.asScala
    val contactAttributes: Set[String] = config.getContactAttributes.asScala
    val accountsDf = lattice.input(0)
    var contactsDf = lattice.input(1)
    if (contactsDf.rdd.isEmpty) {
      contactsDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], getSchema(BusinessEntity.Contact))
    }
    var targetSegmentsContactsDF = lattice.input(2)
    if (targetSegmentsContactsDF.rdd.isEmpty()) {
      targetSegmentsContactsDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], getSchema(BusinessEntity.Contact))
    }
    var negativeDeltaDf = lattice.input(3)
    if (negativeDeltaDf.rdd.isEmpty()) {
      negativeDeltaDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], getSchema(mainEntity))
    }
    var positiveDeltaDf = lattice.input(4)
    if (positiveDeltaDf.rdd.isEmpty()) {
      positiveDeltaDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], getSchema(mainEntity))
    }
    val perAccountLimitedContactsDf = lattice.input(5)
    var distinctPositiveAccountsDf = positiveDeltaDf
    var distinctNegativeAccountsDf = negativeDeltaDf
    enrichAttributes(accountsDf, accountAttributes)
    enrichAttributes(contactsDf, contactAttributes)
    enrichAttributes(targetSegmentsContactsDF, contactAttributes)
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
      val removedContactsData = contactsDf.drop(accountId).join(negativeDeltaDf, Seq(contactId), "right")

      if (CDLExternalSystemName.AD_PLATFORMS.contains(externalSystemName)) {
        addedContactsData = CountryCodeUtils.convert(addedContactsData, contactCountry, contactCountry, url, user, password, key, salt)
        fullContactsData = CountryCodeUtils.convert(fullContactsData, contactCountry, contactCountry, url, user, password, key, salt)
      }

      if (config.useContactsPerAccountLimit) {
        fullContactsData = fullContactsData.drop(accountId).join(perAccountLimitedContactsDf, Seq(contactId), "inner").select(fullContactsData + ".*")
      }

      lattice.output = List(addedAccountsData, removedAccountsData, fullContactsData, addedContactsData, removedContactsData)
    }
  }

  private def enrichAttributes(input: DataFrame, attributes: Set[String]): DataFrame = {
    var result: DataFrame = input
    val columnsExist: ListBuffer[String] = ListBuffer()
    val columnsNotExist: ListBuffer[String] = ListBuffer()
    attributes.foreach { attribute =>
      if (input.columns.contains(attribute)) {
        columnsExist += attribute
      } else {
        columnsNotExist += attribute
      }
    }
    columnsNotExist.map(accountColumn => result = result.withColumn(accountColumn, lit(null).cast(StringType)))
    result
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
