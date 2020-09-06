package com.latticeengines.spark.exposed.job.cdl

import java.io.ByteArrayOutputStream

import com.latticeengines.common.exposed.util.{CipherUtils, JsonUtils, KryoUtils}
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName
import com.latticeengines.domain.exposed.playmakercore.{NonStandardRecColumnName, RecommendationColumnName}
import com.latticeengines.domain.exposed.pls.DeltaCampaignLaunchSparkContext
import com.latticeengines.domain.exposed.spark.cdl.CreateDeltaRecommendationConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.DeltaCampaignLaunchUtils
import org.apache.spark.sql.functions.{col, count, from_unixtime, lit, sum, to_timestamp, when}
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

    // Read Input, the order matters!

    val listSize = lattice.input.size
    logSpark(s"input size is: $listSize")

    // 0: addAccountTable
    val addAccountTable: DataFrame = lattice.input(0)
    if (!addAccountTable.rdd.isEmpty) {
        logDataFrame("addAccountTable", addAccountTable, joinKey, Seq(joinKey), limit = 30)
        addAccountTable.printSchema
    }
    // 1: addContactTable
    val addContactTable: DataFrame = lattice.input(1)
    if (!addContactTable.rdd.isEmpty) {
        logDataFrame("addContactTable", addContactTable, joinKey, Seq(joinKey), limit = 30)
        addContactTable.printSchema
    }
    // 2: deleteAccountTable
    val deleteAccountTable: DataFrame = lattice.input(2)
    if (!deleteAccountTable.rdd.isEmpty) {
        logDataFrame("deleteAccountTable", deleteAccountTable, joinKey, Seq(joinKey), limit = 30)
        deleteAccountTable.printSchema
    }
    // 3: deleteContactTable
    val deleteContactTable: DataFrame = lattice.input(3)
    if (!deleteContactTable.rdd.isEmpty) {
        logDataFrame("deleteContactTable", deleteContactTable, joinKey, Seq(joinKey), limit = 30)
        deleteContactTable.printSchema
    }
    // 4: completeContactTable
    val completeContactTable: DataFrame = lattice.input(4)
    if (!completeContactTable.rdd.isEmpty) {
        logDataFrame("completeContactTable", completeContactTable, joinKey, Seq(joinKey), limit = 30)
        completeContactTable.printSchema
    }

    var finalDfs = new ListBuffer[DataFrame]()
    val contactNums = new Array[Long](2)
    if (createRecommendationDataFrame) {
      val recommendationDfWithContactNum: DataFrame = createRecommendationDf(spark, deltaCampaignLaunchSparkContext,
        addAccountTable, completeContactTable, CDLExternalSystemName.Salesforce.name().equals(deltaCampaignLaunchSparkContext.getDestinationSysName))
      val recContactCount = recommendationDfWithContactNum.agg(sum("CONTACT_NUM")).first.get(0)
      contactNums(0) = if (recContactCount != null) recContactCount.toString.toLong else 0L
      val recommendationDf = recommendationDfWithContactNum.drop("CONTACT_NUM").checkpoint(eager = true)
      if (deltaCampaignLaunchSparkContext.getPublishRecommendationsForS3Launch) {
        exportToRecommendationTable(deltaCampaignLaunchSparkContext, recommendationDf)
      }
      finalDfs += recommendationDf
      if (createAddCsvDataFrame) {
        var finalRecommendationDf: DataFrame = recommendationDf
        if (!addContactTable.rdd.isEmpty) {
          // replace contacts
          val aggregatedContacts = aggregateContacts(addContactTable, contactCols, sfdcContactId, joinKey, false)
          finalRecommendationDf = recommendationDf.drop("CONTACTS").withColumnRenamed("ACCOUNT_ID", joinKey).join(aggregatedContacts, joinKey :: Nil, "left").withColumnRenamed(joinKey, "ACCOUNT_ID")
          val contactCount = finalRecommendationDf.agg(sum("CONTACT_NUM")).first.get(0)
          contactNums(0) = if (contactCount != null) contactCount.toString.toLong else 0L
        } else {
          // add empty CONTACTS column
          finalRecommendationDf = recommendationDf.drop("CONTACTS").withColumn("CONTACTS", lit(""))
          contactNums(0) = 0L
        }

        val addCsvDataFrame: DataFrame = generateUserConfiguredDataFrame(finalRecommendationDf, addAccountTable, deltaCampaignLaunchSparkContext, joinKey)
        finalDfs += addCsvDataFrame
      }
    }

    if (createDeleteCsvDataFrame) {
        val deleteRecDfWithContactNum: DataFrame = createRecommendationDf(spark, deltaCampaignLaunchSparkContext,
          deleteAccountTable, deleteContactTable, false)
        val deleteContactCount = deleteRecDfWithContactNum.agg(sum("CONTACT_NUM")).first.get(0)
        contactNums(1) = if (deleteContactCount != null) deleteContactCount.toString.toLong else 0L
        val deleteRecDf = deleteRecDfWithContactNum.drop("CONTACT_NUM")
        val deleteCsvDataFrame: DataFrame = generateUserConfiguredDataFrame(deleteRecDf, deleteAccountTable, deltaCampaignLaunchSparkContext, joinKey)
        finalDfs += deleteCsvDataFrame
    }
    lattice.output = finalDfs.toList
    lattice.outputStr = contactNums.mkString("[", ",", "]")
  }

  // returns recommendation table with one more column "CONTACT_NUM". Need to drop that column before further processing
  private def createRecommendationDf(spark: SparkSession, deltaCampaignLaunchSparkContext: DeltaCampaignLaunchSparkContext,
                                     addAccountTable: DataFrame, completeContactTable: DataFrame, onlyPopulateOneContact: Boolean): DataFrame = {
      val joinKey: String = deltaCampaignLaunchSparkContext.getJoinKey
      val contactCols: Seq[String] = if (deltaCampaignLaunchSparkContext.getContactCols != null) deltaCampaignLaunchSparkContext.getContactCols.asScala else Seq.empty[String]
      val sfdcContactId: String = deltaCampaignLaunchSparkContext.getSfdcContactID

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

      var finalRecommendations: DataFrame = null
      if (!completeContactTable.rdd.isEmpty) {
          val aggregatedContacts = aggregateContacts(completeContactTable, contactCols, sfdcContactId, joinKey, onlyPopulateOneContact)
          val recommendations = derivedAccounts.join(aggregatedContacts, joinKey :: Nil, "left")

          logDataFrame("recommendations", recommendations, joinKey, Seq(joinKey, "CONTACT_NUM"), limit = 100)
          finalRecommendations = recommendations.withColumnRenamed(joinKey,"ACCOUNT_ID")
      } else {
          // join
          val recommendations = derivedAccounts.withColumn("CONTACTS", lit("")).withColumn("CONTACT_NUM", lit(0))
          finalRecommendations = recommendations.withColumnRenamed(joinKey, "ACCOUNT_ID")
      }
      logDataFrame("finalRecommendations", finalRecommendations, "ACCOUNT_ID", Seq("ACCOUNT_ID", "EXTERNAL_ID"), limit = 100)
      finalRecommendations
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

  private def generateUserConfiguredDataFrame(finalRecommendations: DataFrame, accountTable: DataFrame, deltaCampaignLaunchSparkContext: DeltaCampaignLaunchSparkContext, joinKey: String): DataFrame = {
    val accountColsRecIncluded: Seq[String] = if (deltaCampaignLaunchSparkContext.getAccountColsRecIncluded != null ) deltaCampaignLaunchSparkContext.getAccountColsRecIncluded.asScala else Seq.empty[String]
    val accountColsRecNotIncludedStd: Seq[String] = if (deltaCampaignLaunchSparkContext.getAccountColsRecNotIncludedStd != null) deltaCampaignLaunchSparkContext.getAccountColsRecNotIncludedStd.asScala else Seq.empty[String]
    val accountColsRecNotIncludedNonStd: Seq[String] = if (deltaCampaignLaunchSparkContext.getAccountColsRecNotIncludedNonStd != null) deltaCampaignLaunchSparkContext.getAccountColsRecNotIncludedNonStd.asScala else Seq.empty[String]
    val contactCols: Seq[String] = if (deltaCampaignLaunchSparkContext.getContactCols != null) deltaCampaignLaunchSparkContext.getContactCols.asScala else Seq.empty[String]
    logSpark("Four categories of column metadata are as follows:")
    println(accountColsRecIncluded)
    println(accountColsRecNotIncludedStd)
    println(accountColsRecNotIncludedNonStd)
    println(contactCols)

    if (accountColsRecIncluded.isEmpty //
        && accountColsRecNotIncludedStd.isEmpty //
        && accountColsRecNotIncludedNonStd.isEmpty //
        && contactCols.isEmpty) {
      logSpark("Four categories are all empty.")
      return finalRecommendations
    }

    // 1. combine Recommendation-contained columns (including Contacts column if required)
    // with Recommendation-not-contained standard columns
    var userConfiguredDataFrame: DataFrame = null
    var recContainedCombinedWithStd: DataFrame = null

    val needContactsColumn = contactCols.nonEmpty
    val containsJoinKey = accountColsRecIncluded.contains(joinKey)
    val joinKeyCol: Option[String] = if (!containsJoinKey) Some(joinKey) else None
    val contactsCol: Option[String] = if (needContactsColumn) Some(RecommendationColumnName.CONTACTS.name) else None
    var internalAppendedCols: Seq[String] = Seq.empty[String]

    if (accountColsRecNotIncludedStd.nonEmpty) {
      internalAppendedCols = (accountColsRecIncluded ++ joinKeyCol ++ contactsCol).toSeq
    } else if (accountColsRecIncluded.nonEmpty && accountColsRecNotIncludedStd.isEmpty) {
      internalAppendedCols = (accountColsRecIncluded ++ contactsCol).toSeq
    } else {
      internalAppendedCols = Seq(RecommendationColumnName.CONTACTS.name)
    }

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
      val selectedAccTable = accountTable.select((accountColsRecNotIncludedStd.:+(joinKey)).map(name => col(name)) : _*)
      if (containsJoinKey) {
        recContainedCombinedWithStd = selectedRecTableTranslated.join(selectedAccTable, joinKey :: Nil, "left")
      } else {
        recContainedCombinedWithStd = selectedRecTableTranslated.join(selectedAccTable, joinKey :: Nil, "left").drop(joinKey)
      }
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

    // 4. Drop Contacts column if not needed
    if (!needContactsColumn) {
      userConfiguredDataFrame = userConfiguredDataFrame.drop(RecommendationColumnName.CONTACTS.name)
    }

    logSpark("----- BEGIN SCRIPT OUTPUT -----")
	  userConfiguredDataFrame.printSchema
	  logSpark("----- END SCRIPT OUTPUT -----")
    userConfiguredDataFrame
  }

  private def aggregateContacts(contactTable: DataFrame, contactCols: Seq[String], sfdcContactId: String, joinKey: String, onlyPopulateOneContact: Boolean): DataFrame = {
    var contactTableToUse: DataFrame = contactTable;
    if (onlyPopulateOneContact) {
      contactTableToUse = contactTableToUse.dropDuplicates(Seq(joinKey))
    }
    val contactWithoutJoinKey = contactTableToUse.drop(joinKey)
    val flattenUdf = new Flatten(contactWithoutJoinKey.schema, contactCols, sfdcContactId)
    val aggregatedContacts = contactTableToUse.groupBy(joinKey).agg( //
      flattenUdf(contactWithoutJoinKey.columns map col: _*).as("CONTACTS"), //
      count(lit(1)).as("CONTACT_NUM") //
    )
    val processedAggrContacts = aggregatedContacts.withColumn("CONTACTS", when(col("CONTACTS").isNull, lit("")).otherwise(col("CONTACTS")))
    //aggregatedContacts.rdd.saveAsTextFile("/tmp/aggregated.txt")
    logSpark("----- BEGIN SCRIPT OUTPUT -----")
    processedAggrContacts.printSchema
    logSpark("----- END SCRIPT OUTPUT -----")
    processedAggrContacts
  }
}
