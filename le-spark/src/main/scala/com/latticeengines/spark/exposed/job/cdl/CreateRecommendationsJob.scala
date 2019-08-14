package com.latticeengines.spark.exposed.job.cdl

import java.io.ByteArrayOutputStream
import java.util.UUID

import com.latticeengines.common.exposed.util.{JsonUtils, KryoUtils}
import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants
import com.latticeengines.domain.exposed.playmakercore.{NonStandardRecColumnName, RecommendationColumnName}
import com.latticeengines.domain.exposed.pls.{PlayLaunchSparkContext, RatingBucketName}
import com.latticeengines.domain.exposed.spark.cdl.CreateRecommendationConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.commons.lang3.{EnumUtils, StringUtils}
import org.apache.spark.sql.functions.{col, count, lit, sum, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
//import collection.mutable._
import collection.JavaConverters._

object CreateRecommendationsJob {

  case class Recommendation(PID: Option[Long], //
                            EXTERNAL_ID: String, //
                            AccountId: String, //
                            LE_ACCOUNT_EXTERNAL_ID: String, //
                            PLAY_ID: String, //
                            LAUNCH_ID: String, //
                            DESCRIPTION: String, //
                            LAUNCH_DATE: Option[Long], //
                            LAST_UPDATED_TIMESTAMP: Option[Long], //
                            MONETARY_VALUE: Option[Double], //
                            LIKELIHOOD: Option[Double], //
                            COMPANY_NAME: String, //
                            SFDC_ACCOUNT_ID: String, //
                            PRIORITY_ID: String, //
                            PRIORITY_DISPLAY_NAME: String, //
                            MONETARY_VALUE_ISO4217_ID: String, //
                            LIFT: Option[Double], //
                            RATING_MODEL_ID: String, //
                            MODEL_SUMMARY_ID: String, //
                            SYNC_DESTINATION: String, //
                            DESTINATION_ORG_ID: String, //
                            DESTINATION_SYS_TYPE: String, //
                            TENANT_ID: Long, //
                            DELETED: Boolean)

  def createRec(account: Row, serializedCtx: String): Recommendation = {

    val playLaunchContext = JsonUtils.deserialize(serializedCtx, classOf[PlayLaunchSparkContext])

    val launchTimestampMillis: Long = playLaunchContext.getLaunchTimestampMillis
    val playId: String = playLaunchContext.getPlayName
    val playLaunchId: String = playLaunchContext.getPlayLaunchId
    val tenantId: Long = playLaunchContext.getTenantPid
    val accountId: String = checkAndGet(account, InterfaceName.AccountId.name)
    val externalAccountId: String = accountId
    val uuid: String = UUID.randomUUID().toString
    val description: String = playLaunchContext.getPlayDescription
    val ratingModelId: String = playLaunchContext.getModelId
    val modelSummaryId: String = playLaunchContext.getModelSummaryId
    
    var synchronizationDestination: String = null
    var destinationOrgId: String = null
    var destinationSysType: String = null
    var score: String = null
    var bucketName: String = null
    var bucket: RatingBucketName = null
    var likelihood: Option[Double] = None
    var expectedValue: String = null
    var monetaryValue: Option[Double] = None
    var priorityId: String = null
    var priorityDisplayName: String = null
    var launchTime: Option[Long] = None

    if (playLaunchContext.getCreated != null) {
      launchTime = Some(playLaunchContext.getCreated.getTime)
    } else {
      launchTime = Some(launchTimestampMillis)
    }

    val sfdcAccountId: String =
      if (playLaunchContext.getSfdcAccountID == null)
        null
      else
        checkAndGet(account, playLaunchContext.getSfdcAccountID)

    var companyName: String = checkAndGet(account, InterfaceName.CompanyName.name)
    if (StringUtils.isBlank(companyName)) {
      companyName = checkAndGet(account, InterfaceName.LDC_Name.name())
    }

    if (playLaunchContext.getRatingId != null) {
      score = checkAndGet(account,
        playLaunchContext.getRatingId + PlaymakerConstants.RatingScoreColumnSuffix)
      bucketName = checkAndGet(account, playLaunchContext.getRatingId, RatingBucketName.getUnscoredBucketName)
      if (EnumUtils.isValidEnum(classOf[RatingBucketName], bucketName)) {
        bucket = RatingBucketName.valueOf(bucketName)
      } else {
        bucket = null
      }
      likelihood = if (StringUtils.isNotEmpty(score)) Some(score.toDouble) else Some(getDefaultLikelihood(bucket))
      expectedValue = checkAndGet(account,
        playLaunchContext.getRatingId + PlaymakerConstants.RatingEVColumnSuffix)
      monetaryValue = if (StringUtils.isNotEmpty(expectedValue)) Some(expectedValue.toDouble) else None
      synchronizationDestination = playLaunchContext.getSynchronizationDestination
      destinationOrgId = playLaunchContext.getDestinationOrgId
      destinationSysType = playLaunchContext.getDestinationSysType

      priorityId =
        if (bucket != null)
          bucket.name()
        else
          null
      priorityDisplayName = bucketName
    }

    Recommendation(None, // PID
      uuid, // EXTERNAL_ID
      accountId, // ACCOUNT_ID
      externalAccountId, // LE_ACCOUNT_EXTERNAL_ID
      playId, // PLAY_ID
      playLaunchId, // LAUNCH_ID
      description, // DESCRIPTION
      launchTime, // LAUNCH_DATE
      launchTime, // LAST_UPDATED_TIMESTAMP
      monetaryValue, // MONETARY_VALUE
      likelihood, // LIKELIHOOD
      companyName, // COMPANY_NAME
      sfdcAccountId, // SFDC_ACCOUNT_ID
      priorityId, // PRIORITY_ID
      priorityDisplayName, // PRIORITY_DISPLAY_NAME
      null, // MONETARY_VALUE_ISO4217_ID
      null, // LIFT
      ratingModelId, // RATING_MODEL_ID
      modelSummaryId, // MODEL_SUMMARY_ID
      synchronizationDestination, // SYNC_DESTINATION
      destinationOrgId, // DESTINATION_ORG_ID
      destinationSysType, //DESTINATION_SYS_TYPE
      tenantId, // TENANT_ID
      DELETED = false // DELETED
    )
  }


  def getDefaultLikelihood(bucket: RatingBucketName): Double = {
    bucket match {
      case null => 0.0
      case RatingBucketName.A => 95.0
      case RatingBucketName.B => 70.0
      case RatingBucketName.C => 40.0
      case RatingBucketName.D => 20.0
      case RatingBucketName.E => 10.0
      case RatingBucketName.F => 5.0
      case _ => throw new UnsupportedOperationException("Unknown bucket " + bucket)
    }
  }

  def checkAndGet(account: Row, field: String, defaultValue: String): String = {
    try {
      val obj = account.get(account.fieldIndex(field))
      if  (obj == null) {
        return defaultValue
      } else {
        return obj.toString()
      }
    } catch {
      case _: IllegalArgumentException => defaultValue
    }
  }

  def checkAndGet(account: Row, field: String): String = {
    checkAndGet(account, field, null)
  }

}

class CreateRecommendationsJob extends AbstractSparkJob[CreateRecommendationConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[CreateRecommendationConfig]): Unit = {
    val config: CreateRecommendationConfig = lattice.config
    val playLaunchContext: PlayLaunchSparkContext = config.getPlayLaunchSparkContext
    val topNCount = playLaunchContext.getTopNCount
    val joinKey: String = playLaunchContext.getJoinKey
    val accountColsRecIncluded: Seq[String] = if (playLaunchContext.getAccountColsRecIncluded != null ) playLaunchContext.getAccountColsRecIncluded.asScala else Seq.empty[String]
    val accountColsRecNotIncludedStd: Seq[String] = if (playLaunchContext.getAccountColsRecNotIncludedStd != null) playLaunchContext.getAccountColsRecNotIncludedStd.asScala else Seq.empty[String]
    val accountColsRecNotIncludedNonStd: Seq[String] = if (playLaunchContext.getAccountColsRecNotIncludedNonStd != null) playLaunchContext.getAccountColsRecNotIncludedNonStd.asScala else Seq.empty[String]
    val contactCols: Seq[String] = if (playLaunchContext.getContactCols != null) playLaunchContext.getContactCols.asScala else Seq.empty[String]

    println("----- BEGIN SCRIPT OUTPUT -----")
	  println(s"joinKey is: $joinKey")
	  println("----- END SCRIPT OUTPUT -----")

    // Read Input
	  val listSize = lattice.input.size
	  println(s"input size is: $listSize")
    val accountTable: DataFrame = lattice.input.head

    // Manipulate Account Table with PlayLaunchContext
    val bos: ByteArrayOutputStream = new ByteArrayOutputStream
    KryoUtils.write(bos, playLaunchContext)
    val serializedCtx = JsonUtils.serialize(playLaunchContext)
    val createRecFunc = (account: Row) => CreateRecommendationsJob.createRec(account, serializedCtx)
    val accountAndPlayLaunch = accountTable.rdd.map(createRecFunc)

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

    var limitedAccountTable = derivedAccounts
    if (topNCount != null) {
      limitedAccountTable = derivedAccounts.limit(topNCount.toString.toInt)
    }

    // Manipulate Contact Table
    var finalRecommendations: DataFrame = null
    var finalOutput: String = null
    
    if (listSize == 2) {
      val contactTable: DataFrame = lattice.input(1)
      val aggregatedContacts = aggregateContacts(contactTable, joinKey)
        
      // join
      val recommendations = limitedAccountTable.join(aggregatedContacts, joinKey :: Nil, "left")
      //recommendations.rdd.saveAsTextFile("/tmp/recommendations.txt")
      println("----- BEGIN SCRIPT OUTPUT -----")
	    recommendations.printSchema
	    println("----- END SCRIPT OUTPUT -----")
      val contactCount = recommendations.agg( //
    	  sum("CONTACT_NUM")
      ).first.get(0)
      finalRecommendations = recommendations//
        .withColumnRenamed(joinKey,"ACCOUNT_ID") //
        .drop("CONTACT_NUM")
      finalOutput = contactCount.toString
    } else {
      // join
      val recommendations = limitedAccountTable.withColumn("CONTACTS", lit("[]").cast(StringType))
      finalRecommendations = recommendations.withColumnRenamed(joinKey,"ACCOUNT_ID")
      finalOutput = "0"
    }
    
    // finish
    val orderedRec = finalRecommendations.select("PID", "EXTERNAL_ID", "ACCOUNT_ID", "LE_ACCOUNT_EXTERNAL_ID", "PLAY_ID", "LAUNCH_ID",
        "DESCRIPTION", "LAUNCH_DATE", "LAST_UPDATED_TIMESTAMP", "MONETARY_VALUE", "LIKELIHOOD", "COMPANY_NAME", "SFDC_ACCOUNT_ID",
        "PRIORITY_ID", "PRIORITY_DISPLAY_NAME", "MONETARY_VALUE_ISO4217_ID", "LIFT", "RATING_MODEL_ID", "MODEL_SUMMARY_ID", "CONTACTS",
        "SYNC_DESTINATION", "DESTINATION_ORG_ID", "DESTINATION_SYS_TYPE", "TENANT_ID", "DELETED")
        
    lattice.outputStr = finalOutput
    // generate dataframe for csv file exporter
    
    // No user configured field mapping is provided. Only generate Recommendation DF
    if (accountColsRecIncluded.isEmpty //
        && accountColsRecNotIncludedStd.isEmpty //
        && accountColsRecNotIncludedNonStd.isEmpty //
        && contactCols.isEmpty) {
      lattice.output = orderedRec :: Nil
    } else {
      val userConfiguredDataFrame = generateUserConfiguredDataFrame(orderedRec, accountTable, playLaunchContext, joinKey)
      lattice.output = List(orderedRec, userConfiguredDataFrame)
    }
  }
  
  private def generateUserConfiguredDataFrame(orderedRec: DataFrame, accountTable: DataFrame, playLaunchContext: PlayLaunchSparkContext, joinKey: String): DataFrame = {
    val accountColsRecIncluded: Seq[String] = if (playLaunchContext.getAccountColsRecIncluded != null ) playLaunchContext.getAccountColsRecIncluded.asScala else Seq.empty[String]
    val accountColsRecNotIncludedStd: Seq[String] = if (playLaunchContext.getAccountColsRecNotIncludedStd != null) playLaunchContext.getAccountColsRecNotIncludedStd.asScala else Seq.empty[String]
    val accountColsRecNotIncludedNonStd: Seq[String] = if (playLaunchContext.getAccountColsRecNotIncludedNonStd != null) playLaunchContext.getAccountColsRecNotIncludedNonStd.asScala else Seq.empty[String]
    val contactCols: Seq[String] = if (playLaunchContext.getContactCols != null) playLaunchContext.getContactCols.asScala else Seq.empty[String]
    
    // 1. combine Recommendation-contained columns (including Contacts column if required)
    // with Recommendation-not-contained standard columns
    var userConfiguredDataFrame: DataFrame = null
    var recContainedCombinedWithStd: DataFrame = null
    
    val needContactsColumn = contactCols.nonEmpty
    val containsJoinKey = accountColsRecIncluded.contains(joinKey)
    val joinKeyCol: Option[String] = if (!containsJoinKey) Some(joinKey) else None
    val contactsCol: Option[String] = if (needContactsColumn) Some(RecommendationColumnName.CONTACTS.name) else None
       
    if (accountColsRecNotIncludedStd.nonEmpty) {
      val internalAppendedCols: Seq[String] = (accountColsRecIncluded ++ joinKeyCol ++ contactsCol).toSeq
      val mappedToRecAppendedCols = internalAppendedCols map {col => RecommendationColumnName.INTERNAL_NAME_TO_RECOMMENDATION_COLUMN_MAP.asScala.getOrElse(col, col)}
      val recColsToInternalNameMap = mappedToRecAppendedCols map {col => {col -> RecommendationColumnName.RECOMMENDATION_COLUMN_TO_INTERNAL_NAME_MAP.asScala.getOrElse(col, col)}} toMap
      val selectedRecTable = orderedRec.select((mappedToRecAppendedCols).map(name => col(name)) : _*)
      // need to translate Recommendation name to internal name
      val selectedRecTableTranslated = selectedRecTable.select(recColsToInternalNameMap.map(x => col(x._1).alias(x._2)).toList : _*)
      // need to add joinKey to the accountColsRecNotIncludedStd for the purpose of join
      val selectedAccTable = accountTable.select((accountColsRecNotIncludedStd.:+(joinKey)).map(name => col(name)) : _*)
      
      if (containsJoinKey) {
        recContainedCombinedWithStd = selectedRecTableTranslated.join(selectedAccTable, joinKey :: Nil, "left")
      } else {
        recContainedCombinedWithStd = selectedRecTableTranslated.join(selectedAccTable, joinKey :: Nil, "left").drop(joinKey)
      }
    } else if (accountColsRecIncluded.nonEmpty && accountColsRecNotIncludedStd.isEmpty) {
      val internalAppendedCols = (accountColsRecIncluded ++ contactsCol).toSeq
      val mappedToRecAppendedCols = internalAppendedCols map {col => RecommendationColumnName.INTERNAL_NAME_TO_RECOMMENDATION_COLUMN_MAP.asScala.getOrElse(col, col)}
      val recColsToInternalNameMap = mappedToRecAppendedCols map {col => {col -> RecommendationColumnName.RECOMMENDATION_COLUMN_TO_INTERNAL_NAME_MAP.asScala.getOrElse(col, col)}} toMap
      val selectedRecTable = orderedRec.select((mappedToRecAppendedCols).map(name => col(name)) : _*)
      recContainedCombinedWithStd = selectedRecTable.select(recColsToInternalNameMap.map(x => col(x._1).alias(x._2)).toList : _*)
    } else {
      println("Currently this scenario is not possible")
      recContainedCombinedWithStd = orderedRec
    }
    
    userConfiguredDataFrame = recContainedCombinedWithStd
    // 2. Combine result of 1 with Recommendation-not-contained non-standard columns
    if (accountColsRecNotIncludedNonStd.nonEmpty) {
      for (name <- accountColsRecNotIncludedNonStd) {
        if (name == NonStandardRecColumnName.DESTINATION_SYS_NAME.name) {
          userConfiguredDataFrame = userConfiguredDataFrame.withColumn(name, lit(playLaunchContext.getDestinationOrgName))
        } else if (name == NonStandardRecColumnName.PLAY_NAME.name) {
          userConfiguredDataFrame = userConfiguredDataFrame.withColumn(name, lit(playLaunchContext.getPlayDisplayName))
        } else if (name == NonStandardRecColumnName.RATING_MODEL_NAME.name) {
          userConfiguredDataFrame = userConfiguredDataFrame.withColumn(name, lit(playLaunchContext.getRatingEngineDisplayName))
        } else if (name == NonStandardRecColumnName.SEGMENT_NAME.name) {
          userConfiguredDataFrame = userConfiguredDataFrame.withColumn(name, lit(playLaunchContext.getSegmentDisplayName))
        }
      }
    }
    return userConfiguredDataFrame
  }
  
  private def aggregateContacts(contactTable: DataFrame, joinKey: String): DataFrame = {
      val contactWithoutJoinKey = contactTable.drop(joinKey)
      val flattenUdf = new Flatten(contactWithoutJoinKey.schema)
      val aggregatedContacts = contactTable.groupBy(joinKey).agg( //
        flattenUdf(contactWithoutJoinKey.columns map col: _*).as("CONTACTS"), //
        count(lit(1)).as("CONTACT_NUM") //
      )
      val processedAggrContacts = aggregatedContacts.withColumn("CONTACTS", when(col("CONTACTS").isNull, lit("[]").cast(StringType)).otherwise(col("CONTACTS")))
      //aggregatedContacts.rdd.saveAsTextFile("/tmp/aggregated.txt")
      println("----- BEGIN SCRIPT OUTPUT -----")
	    processedAggrContacts.printSchema
	    println("----- END SCRIPT OUTPUT -----")
      return processedAggrContacts
  }
  
}
