package com.latticeengines.spark.exposed.job.cdl

import java.io.ByteArrayOutputStream
import java.util.UUID

import com.latticeengines.common.exposed.util.{JsonUtils, KryoUtils}
import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants
import com.latticeengines.domain.exposed.pls.{AIModel, PlayLaunch, PlayLaunchSparkContext, RatingBucketName, RatingEngineType}
import com.latticeengines.domain.exposed.spark.cdl.CreateRecommendationConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.commons.lang3.{EnumUtils, StringUtils}
import org.apache.spark.sql.functions.{col, count, lit, sum, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.commons.lang3.{EnumUtils, StringUtils}
import org.apache.spark.sql.functions.{col, count, lit, sum, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}



object Sqoop {

  case class Recommendation(PID: Option[Long], //
                            EXTERNAL_ID: String, //
                            ACCOUNT_ID: String, //
                            LE_ACCOUNT_EXTERNAL_ID: String, //
                            PLAY_ID: String, //
                            LAUNCH_ID: String, //
                            DESCRIPTION: String, //
                            LAUNCH_DATE: Long, //
                            LAST_UPDATED_TIMESTAMP: Long, //
                            MONETARY_VALUE: Double, //
                            LIKELIHOOD: Double, //
                            COMPANY_NAME: String, //
                            SFDC_ACCOUNT_ID: String, //
                            PRIORITY_ID: String, //
                            PRIORITY_DISPLAY_NAME: String, //
                            MONETARY_VALUE_ISO4217_ID: String, //
                            LIFT: String, //
                            RATING_MODEL_ID: String, //
                            MODEL_SUMMARY_ID: String, //
                            CONTACTS: String, //
                            SYNC_DESTINATION: String, //
                            DESTINATION_ORG_ID: String, //
                            DESTINATION_SYS_TYPE: String, //
                            TENANT_ID: Long, //
                            DELETED: Boolean)

  def createRec(account: Row): Recommendation = {
    
    val playId: String = "playId"
    val playLaunchId: String = "playLaunchId"
    val tenantId: Long = 1L
    val accountId: String = "accountId"
    val externalAccountId: String = accountId
    val uuid: String = UUID.randomUUID().toString
    val description: String = "description"

    var synchronizationDestination: String = "synchronizationDestination"
    var destinationOrgId: String = "destinationOrgId"
    var destinationSysType: String = "destinationSysType"
    var score: String = ""
    var bucketName: String = "A"
    var bucket: RatingBucketName = null
    var likelihood: Double = 98.0
    var expectedValue: String = null
    var monetaryValue: Double = 100.0
    var priorityId: String = "A"
    var priorityDisplayName: String = "A"
    var ratingModelId: String = null
    var modelSummaryId: String = "modelSummaryId"
    var launchTime: Long = 1562181990314L
    val sfdcAccountId: String = "sfdcAccountId"
    var companyName: String = "companyName"
    
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
      "[]", // CONTACTS
      synchronizationDestination, // SYNC_DESTINATION
      destinationOrgId, // DESTINATION_ORG_ID
      destinationSysType, //DESTINATION_SYS_TYPE
      tenantId, // TENANT_ID
      DELETED = false // DELETED
    )
  }

}

class Sqoop extends AbstractSparkJob[CreateRecommendationConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[CreateRecommendationConfig]): Unit = {
    // Manipulate Account Table with PlayLaunchContext
    val createRecFunc = (account: Row) => Sqoop.createRec(account)
    val accountTable: DataFrame = lattice.input.head
    val accountAndPlayLaunch = accountTable.rdd.map(createRecFunc)

    val derivedAccounts = spark.createDataFrame(accountAndPlayLaunch) //
      .toDF("PID", //
      "EXTERNAL_ID", //
      "ACCOUNT_ID", //
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
      "CONTACTS", //
      "SYNC_DESTINATION", //
      "DESTINATION_ORG_ID", //
      "DESTINATION_SYS_TYPE", //
      "TENANT_ID", //
      "DELETED")
    
    // finish
    val orderedRec = derivedAccounts.select("PID", "EXTERNAL_ID", "ACCOUNT_ID", "LE_ACCOUNT_EXTERNAL_ID", "PLAY_ID", "LAUNCH_ID",
        "DESCRIPTION", "LAUNCH_DATE", "LAST_UPDATED_TIMESTAMP", "MONETARY_VALUE", "LIKELIHOOD", "COMPANY_NAME", "SFDC_ACCOUNT_ID",
        "PRIORITY_ID", "PRIORITY_DISPLAY_NAME", "MONETARY_VALUE_ISO4217_ID", "LIFT", "RATING_MODEL_ID", "MODEL_SUMMARY_ID", "CONTACTS",
        "SYNC_DESTINATION", "DESTINATION_ORG_ID", "DESTINATION_SYS_TYPE", "TENANT_ID", "DELETED")
//    lattice.output = orderedRec :: Nil    
    
    val values = List(1L, // 1
                 "uuid", // 2
                 "accountId", // 3
                 "accountId", // 4
                 "playId", // 5
                 "launchId", // 6
                 "description",// 7
                 1562181990314L, // 8
                 1562181990314L, // 9
                 100.0, // 10
                 98.0, // 11  
                 "Lattice", // 12
                 "sfdc_ccount", // 13
                 "A", // 14
                 "A", // 15
                 "money_value", // 16
                 "2.0", // 17
                 "rating", // 18
                 "model", // 19
                 "[]", // 20
                 "SFDC", // 21
                 "DES_ORD", // 22
                 "DES_TYPE", // 23
                 1L, // 24
                 false// 25
                     )

    // Create `Row` from `Seq`
    val row = Row.fromSeq(values)

    // Create `RDD` from `Row`
    val rdd = spark.sparkContext.makeRDD(List(row))

    // Create schema fields
    val fields = List(
      StructField("PID", LongType, nullable = false),
      StructField("EXTERNAL_ID", StringType, nullable = false),
      StructField("ACCOUNT_ID", StringType, nullable = false),
      StructField("LE_ACCOUNT_EXTERNAL_ID", StringType, nullable = false),
      StructField("PLAY_ID", StringType, nullable = false),
      StructField("LAUNCH_ID", StringType, nullable = false),
      StructField("DESCRIPTION", StringType, nullable = false),
      StructField("LAUNCH_DATE", LongType, nullable = false),
      StructField("LAST_UPDATED_TIMESTAMP", LongType, nullable = false),
      StructField("MONETARY_VALUE", DoubleType, nullable = false),
      StructField("LIKELIHOOD", DoubleType, nullable = false),
      StructField("COMPANY_NAME", StringType, nullable = false),
      StructField("SFDC_ACCOUNT_ID", StringType, nullable = false),
      StructField("PRIORITY_ID", StringType, nullable = false),
      StructField("PRIORITY_DISPLAY_NAME", StringType, nullable = false),
      StructField("MONETARY_VALUE_ISO4217_ID", StringType, nullable = false),
      StructField("LIFT", StringType, nullable = false),
      StructField("RATING_MODEL_ID", StringType, nullable = false),
      StructField("MODEL_SUMMARY_ID", StringType, nullable = false),
      StructField("CONTACTS", StringType, nullable = false),
      StructField("SYNC_DESTINATION", StringType, nullable = false),
      StructField("DESTINATION_ORG_ID", StringType, nullable = false),
      StructField("DESTINATION_SYS_TYPE", StringType, nullable = false),
      StructField("TENANT_ID", LongType, nullable = false),
      StructField("DELETED", BooleanType, nullable = false)
    )

    // Create `DataFrame`
    val dataFrame = spark.createDataFrame(rdd, StructType(fields))
    lattice.output = dataFrame :: Nil
    lattice.outputStr = ""
  }
  
}
