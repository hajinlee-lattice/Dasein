package com.latticeengines.spark.exposed.job.cdl

import java.io.ByteArrayOutputStream

import com.latticeengines.common.exposed.util.{CipherUtils, JsonUtils, KryoUtils}
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName
import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.playmakercore.{NonStandardRecColumnName, RecommendationColumnName}
import com.latticeengines.domain.exposed.pls.DeltaCampaignLaunchSparkContext
import com.latticeengines.domain.exposed.serviceflows.cdl.DeltaCampaignLaunchWorkflowConfiguration
import com.latticeengines.domain.exposed.spark.cdl.CreateDeltaRecommendationConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.DeltaCampaignLaunchUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, from_unixtime, lit, rank, sum, to_timestamp, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class CreateDeltaRecommendationsJob extends AbstractSparkJob[CreateDeltaRecommendationConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[CreateDeltaRecommendationConfig]): Unit = {
    val config: CreateDeltaRecommendationConfig = lattice.config
    val deltaCampaignLaunchSparkContext: DeltaCampaignLaunchSparkContext = config.getDeltaCampaignLaunchSparkContext
    val joinKey: String = deltaCampaignLaunchSparkContext.getJoinKey
    val playId: String = deltaCampaignLaunchSparkContext.getPlayName
    val playLaunchId: String = deltaCampaignLaunchSparkContext.getPlayLaunchId
    val contactCols: Seq[String] = if (deltaCampaignLaunchSparkContext.getContactCols != null) deltaCampaignLaunchSparkContext.getContactCols.asScala else Seq.empty[String]
    val createRecommendationDataFrame: Boolean = deltaCampaignLaunchSparkContext.getCreateRecommendationDataFrame
    val createAddCsvDataFrame: Boolean = deltaCampaignLaunchSparkContext.getCreateAddCsvDataFrame
    val createDeleteCsvDataFrame: Boolean = deltaCampaignLaunchSparkContext.getCreateDeleteCsvDataFrame
    val sfdcContactId: String = deltaCampaignLaunchSparkContext.getSfdcContactID
    logSpark(f"playId=$playId%s, playLaunchId=$playLaunchId%s, createRecommendationDataFrame=$createRecommendationDataFrame%s, createAddCsvDataFrame=$createAddCsvDataFrame%s, createDeleteCsvDataFrame=$createDeleteCsvDataFrame%s")
    val listSize = lattice.input.size
    logSpark(s"input size is: $listSize")

    // 0: addAccountTable
    val addAccountTable: DataFrame = lattice.input(0)
    printTable("addAccountTable", joinKey, addAccountTable)
    // 1: addContactTable
    val addContactTable: DataFrame = lattice.input(1)
    printTable("addContactTable", joinKey, addContactTable)
    // 2: deleteAccountTable
    val deleteAccountTable: DataFrame = lattice.input(2)
    printTable("deleteAccountTable", joinKey, deleteAccountTable)
    // 3: deleteContactTable
    val deleteContactTable: DataFrame = lattice.input(3)
    printTable("deleteContactTable", joinKey, deleteContactTable)
    // 4: completeContactTable
    val completeContactTable: DataFrame = lattice.input(4)
    printTable("completeContactTable", joinKey, completeContactTable)

    var finalDfs = new ListBuffer[DataFrame]()
    val contactNums = new ListBuffer[Long]()
    val dbConnector: Boolean = deltaCampaignLaunchSparkContext.getPublishRecommendationsToDB && !CDLExternalSystemName.AWS_S3.name().equals(deltaCampaignLaunchSparkContext.getDestinationSysName)
    if (createRecommendationDataFrame) {
      val recommendationDf: DataFrame = createRecommendationDf(spark, deltaCampaignLaunchSparkContext, addAccountTable)
      val baseAddRecDf = recommendationDf.checkpoint(eager = true)
      val result = publishRecommendationsToDB(deltaCampaignLaunchSparkContext, completeContactTable, baseAddRecDf, sfdcContactId, joinKey, contactNums)
      finalDfs += result
      if (createAddCsvDataFrame) {
        if (dbConnector) {
          finalDfs += generateCsvDfForDbConnector(completeContactTable, baseAddRecDf, joinKey, contactCols)
        } else {
          var addRecDf: DataFrame = createFinalRecommendationDf(deltaCampaignLaunchSparkContext, contactCols, contactNums, joinKey, baseAddRecDf, addAccountTable, addContactTable)
          finalDfs += addRecDf
        }
      }
    }
    if (createDeleteCsvDataFrame) {
      var deleteRecDf: DataFrame = createRecommendationDf(spark, deltaCampaignLaunchSparkContext, deleteAccountTable)
      if (dbConnector) {
        finalDfs += generateCsvDfForDbConnector(deleteContactTable, deleteRecDf, joinKey, contactCols)
      } else {
        deleteRecDf = deleteRecDf.checkpoint(eager = true)
        deleteRecDf = createFinalRecommendationDf(deltaCampaignLaunchSparkContext, contactCols, contactNums, joinKey, deleteRecDf, deleteAccountTable, deleteContactTable)
        finalDfs += deleteRecDf
      }
    }
    lattice.output = finalDfs.toList
    lattice.outputStr = contactNums.mkString("[", ",", "]")
  }

  private def generateCsvDfForDbConnector(contactTable: DataFrame, recommendationDF: DataFrame, joinKey: String, contactCols: Seq[String]): DataFrame ={
    var result: DataFrame = recommendationDF
    if (!contactTable.rdd.isEmpty) {
      val columnsExistInContactCols: Seq[String] = contactCols.filter(name => contactTable.columns.contains(name))
      val joinKeyCol: Option[String] = Some(joinKey)
      val contactTableToJoin: DataFrame = contactTable.select((columnsExistInContactCols ++ joinKeyCol).map(name => col(name)): _*)
      val newAttrs = contactTableToJoin.columns.map(c => DeltaCampaignLaunchWorkflowConfiguration.CONTACT_ATTR_PREFIX + c)
      val contactTableRenamed: DataFrame = contactTableToJoin.toDF(newAttrs: _*)
      result.drop("PID")
      result.drop("DELETED")
      result = result.join(contactTableRenamed, result(joinKey) === contactTableRenamed(DeltaCampaignLaunchWorkflowConfiguration.CONTACT_ATTR_PREFIX + joinKey), "left")
    }
    result
  }

  private def createFinalRecommendationDf(deltaCampaignLaunchSparkContext: DeltaCampaignLaunchSparkContext, contactCols: Seq[String],
                                          contactNums: ListBuffer[Long], joinKey: String, recDf: DataFrame, accountTable: DataFrame, contactTable: DataFrame): DataFrame = {
    var result: DataFrame = generateUserConfiguredDataFrame(recDf, accountTable, deltaCampaignLaunchSparkContext, joinKey)
    if (!contactTable.rdd.isEmpty && !contactCols.isEmpty) {
      result = joinContacts(result, contactTable, contactCols, joinKey)
      contactNums += result.filter(col(DeltaCampaignLaunchWorkflowConfiguration.CONTACT_ATTR_PREFIX + InterfaceName.ContactId.name()).isNotNull).count()
    } else {
      result = result.withColumn(DeltaCampaignLaunchWorkflowConfiguration.CONTACT_ATTR_PREFIX + InterfaceName.ContactId.name(), lit(null).cast(StringType))
      contactNums += 0L
    }
    dropJoinKeyIfNeeded(deltaCampaignLaunchSparkContext, joinKey, result)
    result
  }

  private def dropJoinKeyIfNeeded(deltaCampaignLaunchSparkContext: DeltaCampaignLaunchSparkContext, joinKey: String, recDf: DataFrame) = {
    val accountColsRecIncluded: Seq[String] = if (deltaCampaignLaunchSparkContext.getAccountColsRecIncluded != null) deltaCampaignLaunchSparkContext.getAccountColsRecIncluded.asScala else Seq.empty[String]
    val containsJoinKey = accountColsRecIncluded.contains(joinKey)
    if (!containsJoinKey) {
      recDf.drop(joinKey)
    }
  }

  private def publishRecommendationsToDB(deltaCampaignLaunchSparkContext: DeltaCampaignLaunchSparkContext, completeContactTable: DataFrame,
                                         baseAddRecDf: DataFrame, sfdcContactId: String, joinKey: String, contactNums: ListBuffer[Long]): DataFrame = {
    if (deltaCampaignLaunchSparkContext.getPublishRecommendationsToDB) {
      var recommendations: DataFrame = null
      val result: DataFrame = baseAddRecDf
      if (!completeContactTable.rdd.isEmpty && !CDLExternalSystemName.AWS_S3.name().equals(deltaCampaignLaunchSparkContext.getDestinationSysName)) {
        val aggregatedContacts = aggregateContacts(completeContactTable, sfdcContactId, joinKey, deltaCampaignLaunchSparkContext.getDestinationSysName)
        recommendations = baseAddRecDf.join(aggregatedContacts, joinKey :: Nil, "left")
        logDataFrame("recommendations", recommendations, joinKey, Seq(joinKey, "CONTACT_NUM"), limit = 100)
        recommendations = recommendations.withColumnRenamed(joinKey, "ACCOUNT_ID")
        val recContactCount = recommendations.agg(sum("CONTACT_NUM")).first.get(0)
        contactNums += (if (recContactCount != null) recContactCount.toString.toLong else 0L)
        recommendations = recommendations.drop("CONTACT_NUM")
      } else {
        recommendations = baseAddRecDf.withColumn("CONTACTS", lit(""))
          .withColumnRenamed(joinKey, "ACCOUNT_ID")
        contactNums += 0L
      }
      exportToRecommendationTable(deltaCampaignLaunchSparkContext, recommendations)
      return result
    }
    baseAddRecDf
  }

  private def printTable(tableName: String, joinKey: String, table: DataFrame) = {
    if (!table.rdd.isEmpty) {
      logDataFrame(tableName, table, joinKey, Seq(joinKey), limit = 30)
      table.printSchema
    }
  }

  // returns recommendation table with one more column "CONTACT_NUM". Need to drop that column before further processing
  private def createRecommendationDf(spark: SparkSession, deltaCampaignLaunchSparkContext: DeltaCampaignLaunchSparkContext, addAccountTable: DataFrame): DataFrame = {
    val joinKey: String = deltaCampaignLaunchSparkContext.getJoinKey
    val bos: ByteArrayOutputStream = new ByteArrayOutputStream
    KryoUtils.write(bos, deltaCampaignLaunchSparkContext)
    val serializedCtx = JsonUtils.serialize(deltaCampaignLaunchSparkContext)
    logSpark(s"serializedCtx is: $serializedCtx")
    val createRecFunc = (account: Row) => DeltaCampaignLaunchUtils.createRec(account, serializedCtx)
    val accountAndPlayLaunch = addAccountTable.rdd.map(createRecFunc)

    val derivedAccounts = spark.createDataFrame(accountAndPlayLaunch) //
      .toDF("PID", //
        "EXTERNAL_ID", //
        "AccountId", //
        "LE_ACCOUNT_EXTERNAL_ID", //
        "PLAY_ID", //
        "LAUNCH_ID", //
        "DESCRIPTION", //
        "LAUNCH_DATE", //
        "LAST_UPDATED_TIMESTAMP", //
        "MONETARY_VALUE", //
        "LIKELIHOOD", //
        "COMPANY_NAME", //
        "SFDC_ACCOUNT_ID", //
        "PRIORITY_ID", //
        "PRIORITY_DISPLAY_NAME", //
        "MONETARY_VALUE_ISO4217_ID", //
        "LIFT", //
        "RATING_MODEL_ID", //
        "MODEL_SUMMARY_ID", //
        "SYNC_DESTINATION", //
        "DESTINATION_ORG_ID", //
        "DESTINATION_SYS_TYPE", //
        "TENANT_ID", //
        "DELETED")

    logDataFrame("derivedAccounts", derivedAccounts, joinKey, Seq(joinKey), limit = 100)
    derivedAccounts
  }

  private def exportToRecommendationTable(deltaCampaignLaunchSparkContext: DeltaCampaignLaunchSparkContext, orderedRec: DataFrame) = {
    val driver = deltaCampaignLaunchSparkContext.getDataDbDriver()
    val url = deltaCampaignLaunchSparkContext.getDataDbUrl()
    val user = deltaCampaignLaunchSparkContext.getDataDbUser()
    val pw = deltaCampaignLaunchSparkContext.getDataDbPassword()
    val saltHint = deltaCampaignLaunchSparkContext.getSaltHint()
    val encryptionKey = deltaCampaignLaunchSparkContext.getEncryptionKey()
    val prop = new java.util.Properties
    prop.setProperty("driver", driver)
    prop.setProperty("user", user)
    prop.setProperty("password", CipherUtils.decrypt(pw, encryptionKey, saltHint))
    val table = "Recommendation"

    //write data from spark dataframe to database
    val orderedRecToDate = transformFromTimestampToDate(orderedRec)
    orderedRecToDate.write.mode("append").jdbc(url, table, prop)
  }

  private def transformFromTimestampToDate(orderedRec: DataFrame): DataFrame = {
    orderedRec.withColumn("LAUNCH_DATE_DATE",  to_timestamp(from_unixtime(col("LAUNCH_DATE")/1000, "MM/dd/yyyy HH:mm:ss"), "MM/dd/yyyy HH:mm:ss")) //
      .withColumn("LAST_UPDATED_TIMESTAMP_DATE", to_timestamp(from_unixtime(col("LAST_UPDATED_TIMESTAMP")/1000, "MM/dd/yyyy HH:mm:ss"), "MM/dd/yyyy HH:mm:ss")) //
      .drop("LAUNCH_DATE") //
      .drop("LAST_UPDATED_TIMESTAMP") //
      .withColumnRenamed("LAUNCH_DATE_DATE", "LAUNCH_DATE") //
      .withColumnRenamed("LAST_UPDATED_TIMESTAMP_DATE", "LAST_UPDATED_TIMESTAMP")
  }

  private def generateUserConfiguredDataFrame(recommendationsDF: DataFrame, accountTable: DataFrame,
                                              deltaCampaignLaunchSparkContext: DeltaCampaignLaunchSparkContext, joinKey: String): DataFrame = {
    var finalRecommendations: DataFrame = recommendationsDF
    val accountColsRecIncluded: Seq[String] = if (deltaCampaignLaunchSparkContext.getAccountColsRecIncluded != null ) deltaCampaignLaunchSparkContext.getAccountColsRecIncluded.asScala else Seq.empty[String]
    val accountColsRecNotIncludedStd: Seq[String] = if (deltaCampaignLaunchSparkContext.getAccountColsRecNotIncludedStd != null) deltaCampaignLaunchSparkContext.getAccountColsRecNotIncludedStd.asScala else Seq.empty[String]
    val accountColsRecNotIncludedNonStd: Seq[String] = if (deltaCampaignLaunchSparkContext.getAccountColsRecNotIncludedNonStd != null) deltaCampaignLaunchSparkContext.getAccountColsRecNotIncludedNonStd.asScala else Seq.empty[String]
    logSpark("Four categories of column metadata are as follows:")
    println(accountColsRecIncluded)
    println(accountColsRecNotIncludedStd)
    println(accountColsRecNotIncludedNonStd)

    if (accountColsRecIncluded.isEmpty //
      && accountColsRecNotIncludedStd.isEmpty //
      && accountColsRecNotIncludedNonStd.isEmpty) {
      logSpark("Four categories are all empty.")
      return finalRecommendations
    }
    finalRecommendations = finalRecommendations.withColumnRenamed(joinKey, "ACCOUNT_ID")
    // 1. combine Recommendation-contained columns (including Contacts column if required)
    // with Recommendation-not-contained standard columns
    var userConfiguredDataFrame: DataFrame = null
    var recContainedCombinedWithStd: DataFrame = null

    val containsJoinKey = accountColsRecIncluded.contains(joinKey)
    val joinKeyCol: Option[String] = if (!containsJoinKey) Some(joinKey) else None
    val internalAppendedCols: Seq[String] = accountColsRecIncluded ++ joinKeyCol

    // map internal column names to Recommendation column names
    val mappedToRecAppendedCols = internalAppendedCols.map{col => RecommendationColumnName.INTERNAL_NAME_TO_RECOMMENDATION_COLUMN_MAP.asScala.getOrElse(col, col)}
    // get the map from Recommendation column names to internal column names for later rename purpose
    val recColsToInternalNameMap = mappedToRecAppendedCols.map{col => {col -> RecommendationColumnName.RECOMMENDATION_COLUMN_TO_INTERNAL_NAME_MAP.asScala.getOrElse(col, col)}}.toMap
    // select columns from Recommendation DataFrame
    val selectedRecTable = finalRecommendations.select((mappedToRecAppendedCols).map(name => col(name)) : _*)
    // translate Recommendation column name to internal column name
    val selectedRecTableTranslated = selectedRecTable.select(recColsToInternalNameMap.map(x => col(x._1).alias(x._2)).toList : _*)
    if (accountColsRecNotIncludedStd.nonEmpty) {
      // 2. need to add joinKey to the accountColsRecNotIncludedStd for the purpose of join
      val selectedAccTable = accountTable.select((accountColsRecNotIncludedStd.:+(joinKey)).map(name => col(name)): _*)
      recContainedCombinedWithStd = selectedRecTableTranslated.join(selectedAccTable, joinKey :: Nil, "left")
    } else {
      recContainedCombinedWithStd = selectedRecTableTranslated
    }

    // 3. Combine result of 1 with Recommendation-not-contained non-standard columns
    userConfiguredDataFrame = recContainedCombinedWithStd
    if (accountColsRecNotIncludedNonStd.nonEmpty) {
      for (name <- accountColsRecNotIncludedNonStd) {
        if (name == NonStandardRecColumnName.DESTINATION_SYS_NAME.name) {
          userConfiguredDataFrame = userConfiguredDataFrame.withColumn(name, lit(deltaCampaignLaunchSparkContext.getDestinationOrgName).cast(StringType))
        } else if (name == NonStandardRecColumnName.PLAY_NAME.name) {
          userConfiguredDataFrame = userConfiguredDataFrame.withColumn(name, lit(deltaCampaignLaunchSparkContext.getPlayDisplayName).cast(StringType))
        } else if (name == NonStandardRecColumnName.RATING_MODEL_NAME.name) {
          userConfiguredDataFrame = userConfiguredDataFrame.withColumn(name, lit(deltaCampaignLaunchSparkContext.getRatingEngineDisplayName).cast(StringType))
        } else if (name == NonStandardRecColumnName.SEGMENT_NAME.name) {
          userConfiguredDataFrame = userConfiguredDataFrame.withColumn(name, lit(deltaCampaignLaunchSparkContext.getSegmentDisplayName).cast(StringType))
        }
      }
    }

    logSpark("----- BEGIN SCRIPT USER CONFIG DF OUTPUT -----")
    userConfiguredDataFrame.printSchema
    logSpark("----- END SCRIPT USER CONFIG DF OUTPUT -----")
    userConfiguredDataFrame
  }

  private def aggregateContacts(contactTable: DataFrame, sfdcContactId: String, joinKey: String, destinationSysName: String): DataFrame= {
    var contactTableToUse: DataFrame = contactTable
    val rowNumber: String = "rowNumber"
    if (CDLExternalSystemName.Salesforce.name().equals(destinationSysName)) {
      contactTableToUse = contactTableToUse.withColumn(rowNumber, rank().over(Window.partitionBy(joinKey).orderBy(InterfaceName.ContactId.name()))).filter(col(rowNumber) <= 1).drop(rowNumber)
    }
    val contactWithoutJoinKey = contactTableToUse.drop(joinKey)
    val flattenUdf = new Flatten(contactWithoutJoinKey.schema, Seq.empty[String], sfdcContactId)
    val aggregatedContacts = contactTableToUse.groupBy(joinKey).agg( //
      flattenUdf(contactWithoutJoinKey.columns map col: _*).as("CONTACTS"), //
      count(lit(1)).as("CONTACT_NUM") //
    )
    val processedAggrContacts = aggregatedContacts.withColumn("CONTACTS", when(col("CONTACTS").isNull, lit("")).otherwise(col("CONTACTS")))
    //aggregatedContacts.rdd.saveAsTextFile("/tmp/aggregated.txt")
    logSpark("----- BEGIN SCRIPT OUTPUT AGGREGATE CONTACTS -----")
    processedAggrContacts.printSchema
    logSpark("----- END SCRIPT OUTPUT AGGREGATE CONTACTS -----")
    processedAggrContacts
  }

  private def joinContacts(accountTable: DataFrame, contactTable: DataFrame, contactCols: Seq[String], joinKey: String): DataFrame = {
    var joinResult: DataFrame = accountTable
    var contactColsToUse: Seq[String] = contactCols
    val containsJoinKey = contactColsToUse.contains(joinKey)
    if (!contactColsToUse.contains(InterfaceName.ContactId.name())) {
      contactColsToUse = contactColsToUse :+ InterfaceName.ContactId.name()
    }
    val joinKeyCol: Option[String] = if (!containsJoinKey) Some(joinKey) else None
    val columnsExistInContactCols: Seq[String] = contactColsToUse.filter(name => contactTable.columns.contains(name))
    val contactTableToJoin: DataFrame = contactTable.select((columnsExistInContactCols ++ joinKeyCol).map(name => col(name)): _*)
    val newAttrs = contactTableToJoin.columns.map(c => DeltaCampaignLaunchWorkflowConfiguration.CONTACT_ATTR_PREFIX + c)
    val contactTableRenamed: DataFrame = contactTableToJoin.toDF(newAttrs: _*)
    joinResult = joinResult.join(contactTableRenamed, joinResult(joinKey) === contactTableRenamed(DeltaCampaignLaunchWorkflowConfiguration.CONTACT_ATTR_PREFIX + joinKey), "left")
    logSpark("----- BEGIN SCRIPT OUTPUT ACCOUNT JOIN CONTACT -----")
    joinResult.printSchema
    logSpark("----- END SCRIPT OUTPUT ACCOUNT JOIN CONTACT -----")
    joinResult
  }
}
