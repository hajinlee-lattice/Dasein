package com.latticeengines.spark.util

import java.util.UUID

import com.latticeengines.common.exposed.util.JsonUtils
import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants
import com.latticeengines.domain.exposed.pls.{PlayLaunchSparkContext, RatingBucketName}
import org.apache.commons.lang3.{EnumUtils, StringUtils}
import org.apache.spark.sql.Row

private[spark] object DeltaCampaignLaunchUtils {

  case class Recommendation(PID: Option[Long], //
                            EXTERNAL_ID: String, //
                            AccountId: String, //
                            CustomerAccountId: String, //
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

  def createRec(account: Row, serializedCtx: String, userCustomerId: Boolean): Recommendation = {

    val playLaunchContext = JsonUtils.deserialize(serializedCtx, classOf[PlayLaunchSparkContext])
    val launchTimestampMillis: Long = playLaunchContext.getLaunchTimestampMillis
    val playId: String = playLaunchContext.getPlayName
    val playLaunchId: String = playLaunchContext.getPlayLaunchId
    val tenantId: Long = playLaunchContext.getTenantPid
    val accountId: String = checkAndGet(account, InterfaceName.AccountId.name)
    val customerAccountId: String = checkAndGet(account, getAccountId(userCustomerId))
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
    var sfdcAccountId: String = null
    var externalAccountId: String = ""

    if (playLaunchContext.getCreated != null) {
      launchTime = Some(playLaunchContext.getCreated.getTime)
    } else {
      launchTime = Some(launchTimestampMillis)
    }

    if (playLaunchContext.getSfdcAccountID == null) {
      sfdcAccountId = checkAndGet(account, getAccountId(playLaunchContext.getIsEntityMatch))
    } else {
      sfdcAccountId = checkAndGet(account, playLaunchContext.getSfdcAccountID)
    }

    if (sfdcAccountId != null) {
      externalAccountId = sfdcAccountId
    }

    var companyName: String = checkAndGet(account, InterfaceName.CompanyName.name)
    if (StringUtils.isBlank(companyName)) {
      companyName = checkAndGet(account, InterfaceName.LDC_Name.name())
    }
    synchronizationDestination = playLaunchContext.getSynchronizationDestination
    destinationOrgId = playLaunchContext.getDestinationOrgId
    destinationSysType = playLaunchContext.getDestinationSysType
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
      priorityId =
        if (bucket != null)
          bucket.name()
        else
          null
      priorityDisplayName = bucketName
    }

    Recommendation(None, // PID
      uuid, // EXTERNAL_ID
      accountId, // AccountId
      customerAccountId, // CustomerAccountId
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

  def getAccountId(userCustomerId: Boolean): String = {
    if (userCustomerId) {
      InterfaceName.CustomerAccountId.name
    } else {
      InterfaceName.AccountId.name
    }
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
        defaultValue
      } else {
        obj.toString()
      }
    } catch {
      case _: IllegalArgumentException => defaultValue
    }
  }

  def checkAndGet(account: Row, field: String): String = {
    checkAndGet(account, field, null)
  }
}
