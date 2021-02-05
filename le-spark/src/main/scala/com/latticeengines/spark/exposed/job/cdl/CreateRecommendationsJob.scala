package com.latticeengines.spark.exposed.job.cdl

import java.io.ByteArrayOutputStream

import com.latticeengines.common.exposed.util.{CipherUtils, JsonUtils, KryoUtils}
import com.latticeengines.domain.exposed.playmakercore.{NonStandardRecColumnName, RecommendationColumnName}
import com.latticeengines.domain.exposed.pls.PlayLaunchSparkContext
import com.latticeengines.domain.exposed.spark.cdl.CreateRecommendationConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.DeltaCampaignLaunchUtils
import org.apache.spark.sql.functions.{col, count, from_unixtime, lit, sum, to_timestamp, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.asc

import scala.collection.JavaConverters._

class CreateRecommendationsJob extends AbstractSparkJob[CreateRecommendationConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[CreateRecommendationConfig]): Unit = {
    val config: CreateRecommendationConfig = lattice.config
    val playLaunchContext: PlayLaunchSparkContext = config.getPlayLaunchSparkContext
    val topNCount = playLaunchContext.getTopNCount
    val joinKey: String = playLaunchContext.getJoinKey
    val playId: String = playLaunchContext.getPlayName
    val playLaunchId: String = playLaunchContext.getPlayLaunchId
    val ratingId: String = playLaunchContext.getRatingId
    val accountColsRecIncluded: Seq[String] = if (playLaunchContext.getAccountColsRecIncluded != null ) playLaunchContext.getAccountColsRecIncluded.asScala else Seq.empty[String]
    val accountColsRecNotIncludedStd: Seq[String] = if (playLaunchContext.getAccountColsRecNotIncludedStd != null) playLaunchContext.getAccountColsRecNotIncludedStd.asScala else Seq.empty[String]
    val accountColsRecNotIncludedNonStd: Seq[String] = if (playLaunchContext.getAccountColsRecNotIncludedNonStd != null) playLaunchContext.getAccountColsRecNotIncludedNonStd.asScala else Seq.empty[String]
    val contactCols: Seq[String] = if (playLaunchContext.getContactCols != null) playLaunchContext.getContactCols.asScala else Seq.empty[String]

	  logSpark(f"playId=$playId%s, playLaunchId=$playLaunchId%s")

    // Read Input
	  val listSize = lattice.input.size
	  logSpark(s"input size is: $listSize")
    val accountTable: DataFrame = lattice.input.head
    logDataFrame("accountTable", accountTable, joinKey, Seq(joinKey), limit = 30)

    val limitedAccountTable = if (topNCount != null) {
        logSpark(s"topNCount is: $topNCount")
        if (ratingId != null) {
          accountTable.sort(asc(ratingId), asc(joinKey)).limit(topNCount.toString.toInt)
        } else {
          accountTable.sort(asc(joinKey)).limit(topNCount.toString.toInt)
        }
    } else {
        accountTable
    }.checkpoint(eager = true)
    
    // Manipulate Account Table with PlayLaunchContext
    val bos: ByteArrayOutputStream = new ByteArrayOutputStream
    KryoUtils.write(bos, playLaunchContext)
    val serializedCtx = JsonUtils.serialize(playLaunchContext)
    logSpark(s"serializedCtx is: $serializedCtx")
    val createRecFunc = (account: Row) => DeltaCampaignLaunchUtils.createRec(account, serializedCtx, false)
    val accountAndPlayLaunch = limitedAccountTable.rdd.map(createRecFunc)

    val derivedAccounts = spark.createDataFrame(accountAndPlayLaunch) //
      .toDF("PID", //
      "EXTERNAL_ID", //
      "AccountId", //
      "CustomerAccountId", //
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
      "DELETED").drop("CustomerAccountId")

    // add log
    logSpark("Before joining, the derivedAccounts is:")
    logDataFrame("derivedAccounts", derivedAccounts, joinKey, Seq(joinKey), limit = 100)

    val derivedAccountsSize = derivedAccounts.count()
    logSpark(s"derivedAccountsSize is: $derivedAccountsSize")

    // Manipulate Contact Table
    var finalRecommendations: DataFrame = null
    var finalOutput: String = null
    
    val sfdcContactId: String = playLaunchContext.getSfdcContactID

    if (listSize == 2) {
      val contactTable: DataFrame = lattice.input(1)
      // calculate the total number of contact
      logSpark("Check again before joining, the derivedAccounts is:")
      logDataFrame("derivedAccounts", derivedAccounts, joinKey, Seq(joinKey), limit = 100)
      val selectedContactNum = contactTable.join(derivedAccounts, joinKey :: Nil, "inner").count()
      logSpark(f"selectedContactNum=$selectedContactNum%d")

      val selectedAccountList =  contactTable.join(derivedAccounts, joinKey :: Nil, "inner").select(joinKey).distinct()
      val selctedContacts = contactTable.join(selectedAccountList, joinKey :: Nil, "inner")
      val aggregatedContacts = aggregateContacts(selctedContacts, contactCols, sfdcContactId, joinKey)

      // join
      val recommendations = derivedAccounts.join(aggregatedContacts, joinKey :: Nil, "left")
      val contactCount = recommendations.agg( //
    	  sum("CONTACT_NUM")
      ).first.get(0)

      logSpark(f"playLaunch_Id=$playLaunchId%s")
      logDataFrame("recommendations", recommendations, joinKey, Seq(joinKey, "CONTACT_NUM"), limit = 100)
      finalRecommendations = recommendations//
        .withColumnRenamed(joinKey,"ACCOUNT_ID") //
        .drop("CONTACT_NUM")

      // add log
      logDataFrame("finalRecommendations", finalRecommendations, "ACCOUNT_ID", Seq("ACCOUNT_ID", "EXTERNAL_ID"), limit = 100)

      if (contactCount != null) {
        finalOutput = contactCount.toString
        // add log
        logSpark(f"contactLaunched=$finalOutput%s")
      } else {
        finalOutput = "0"
      }
    } else {
      // join
      val recommendations = derivedAccounts.withColumn("CONTACTS", lit(""))
      finalRecommendations = recommendations.withColumnRenamed(joinKey, "ACCOUNT_ID")
      finalOutput = "0"
    }

    lattice.outputStr = finalOutput
    val finalRecommendationsSaved = finalRecommendations.checkpoint(eager = true)
    logDataFrame("finalRecommendationsSaved", finalRecommendationsSaved, "ACCOUNT_ID", Seq("ACCOUNT_ID", "EXTERNAL_ID"), limit = 100)

    // No user configured field mapping is provided. Only generate Recommendation DF
    if (accountColsRecIncluded.isEmpty //
        && accountColsRecNotIncludedStd.isEmpty //
        && accountColsRecNotIncludedNonStd.isEmpty //
        && contactCols.isEmpty) {
      lattice.output = List(finalRecommendationsSaved, finalRecommendationsSaved)
    } else {
      // Generate dataframe for csv file exporter
      val userConfiguredDataFrame = generateUserConfiguredDataFrame(finalRecommendationsSaved, accountTable, playLaunchContext, joinKey)
      lattice.output = List(finalRecommendationsSaved, userConfiguredDataFrame)
    }
    
    exportToRecommendationTable(playLaunchContext, finalRecommendationsSaved)
  }
  
  private def exportToRecommendationTable(playLaunchContext: PlayLaunchSparkContext, orderedRec: DataFrame) = {
    val driver = playLaunchContext.getDataDbDriver()
    val url = playLaunchContext.getDataDbUrl()
    val user = playLaunchContext.getDataDbUser()
    val pw = playLaunchContext.getDataDbPassword()
    val saltHint = playLaunchContext.getSaltHint()
    val encryptionKey = playLaunchContext.getEncryptionKey()
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
    return orderedRec.withColumn("LAUNCH_DATE_DATE",  to_timestamp(from_unixtime(col("LAUNCH_DATE")/1000, "MM/dd/yyyy HH:mm:ss"), "MM/dd/yyyy HH:mm:ss")) //
                     .withColumn("LAST_UPDATED_TIMESTAMP_DATE", to_timestamp(from_unixtime(col("LAST_UPDATED_TIMESTAMP")/1000, "MM/dd/yyyy HH:mm:ss"), "MM/dd/yyyy HH:mm:ss")) //
                     .drop("LAUNCH_DATE") //
                     .drop("LAST_UPDATED_TIMESTAMP") //
                     .withColumnRenamed("LAUNCH_DATE_DATE", "LAUNCH_DATE") //
                     .withColumnRenamed("LAST_UPDATED_TIMESTAMP_DATE", "LAST_UPDATED_TIMESTAMP")
  }

  private def generateUserConfiguredDataFrame(finalRecommendations: DataFrame, accountTable: DataFrame, playLaunchContext: PlayLaunchSparkContext, joinKey: String): DataFrame = {
    val accountColsRecIncluded: Seq[String] = if (playLaunchContext.getAccountColsRecIncluded != null ) playLaunchContext.getAccountColsRecIncluded.asScala else Seq.empty[String]
    val accountColsRecNotIncludedStd: Seq[String] = if (playLaunchContext.getAccountColsRecNotIncludedStd != null) playLaunchContext.getAccountColsRecNotIncludedStd.asScala else Seq.empty[String]
    val accountColsRecNotIncludedNonStd: Seq[String] = if (playLaunchContext.getAccountColsRecNotIncludedNonStd != null) playLaunchContext.getAccountColsRecNotIncludedNonStd.asScala else Seq.empty[String]
    val contactCols: Seq[String] = if (playLaunchContext.getContactCols != null) playLaunchContext.getContactCols.asScala else Seq.empty[String]
    logSpark("Four categories of column metadata are as follows:")
    println(accountColsRecIncluded)
    println(accountColsRecNotIncludedStd)
    println(accountColsRecNotIncludedNonStd)
    println(contactCols)

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
    // select columns from Recommendation Dataframe
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
          userConfiguredDataFrame = userConfiguredDataFrame.withColumn(name, lit(playLaunchContext.getDestinationOrgName).cast(StringType))
        } else if (name == NonStandardRecColumnName.PLAY_NAME.name) {
          userConfiguredDataFrame = userConfiguredDataFrame.withColumn(name, lit(playLaunchContext.getPlayDisplayName).cast(StringType))
        } else if (name == NonStandardRecColumnName.RATING_MODEL_NAME.name) {
          userConfiguredDataFrame = userConfiguredDataFrame.withColumn(name, lit(playLaunchContext.getRatingEngineDisplayName).cast(StringType))
        } else if (name == NonStandardRecColumnName.SEGMENT_NAME.name) {
          userConfiguredDataFrame = userConfiguredDataFrame.withColumn(name, lit(playLaunchContext.getSegmentDisplayName).cast(StringType))
        }
      }
    }

    // 4. Drop Contacts column if not needed
    if (!needContactsColumn) {
      userConfiguredDataFrame = userConfiguredDataFrame.drop(RecommendationColumnName.CONTACTS.name)
    }

    userConfiguredDataFrame
  }

  private def aggregateContacts(contactTable: DataFrame, contactCols: Seq[String], sfdcContactId: String, joinKey: String): DataFrame = {
      val contactWithoutJoinKey = contactTable.drop(joinKey)
      val flattenUdf = new Flatten(contactWithoutJoinKey.schema, contactCols, sfdcContactId, false, false, false)
      val aggregatedContacts = contactTable.groupBy(joinKey).agg( //
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