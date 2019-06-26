package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.spark.cdl.CreateRecommendationConfig
import com.latticeengines.domain.exposed.pls.PlayLaunchSparkContext
import com.latticeengines.domain.exposed.pls.Play
import com.latticeengines.domain.exposed.pls.PlayLaunch
import com.latticeengines.domain.exposed.pls.RatingModel
import com.latticeengines.domain.exposed.pls.RatingEngine
import com.latticeengines.domain.exposed.pls.AIModel
import com.latticeengines.domain.exposed.pls.RatingEngineType
import com.latticeengines.domain.exposed.pls.RatingBucketName
import com.latticeengines.domain.exposed.security.Tenant
import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants

import java.util.UUID
import java.util.Date

import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.EnumUtils

import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

class JoinJob extends AbstractSparkJob[CreateRecommendationConfig] {

	case class Recommendation(PID: Option[Long], //
	                          EXTERNAL_ID: String, //
	                          ACCOUNT_ID: String, //
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
	                          LIFT: String, //
	                          RATING_MODEL_ID: String, //
	                          MODEL_SUMMARY_ID: String, //
	                          SYNC_DESTINATION: String, //
	                          DESTINATION_ORG_ID: String, //
	                          DESTINATION_SYS_TYPE: String, //
	                          TENANT_ID: Long, //
	                          DELETED: Boolean)

  def createRec(account: Row, playLaunchContext: PlayLaunchSparkContext): Recommendation = {
  
    val playLaunch: PlayLaunch = playLaunchContext.getPlayLaunch
	val launchTimestampMillis: Long = playLaunchContext.getLaunchTimestampMillis
	val playId: String = playLaunchContext.getPlayName
	val playLaunchId: String = playLaunchContext.getPlayLaunchId
	val tenantId: Long = playLaunchContext.getTenant().getPid()
	val accountId :String = checkAndGet(account, InterfaceName.AccountId.name())
	val externalAccountId: String = accountId
	val uuid: String = UUID.randomUUID().toString()
	val description: String = playLaunch.getPlay().getDescription()
	
	var synchronizationDestination: String = null
	var destinationOrgId: String = null
	var destinationSysType: String = null
	var score: String = null
	var bucketName: String = null
	var bucket: RatingBucketName = null
	var likelihood: Option[Double] = None
	var expectedValue:String = null
	var monetaryValue: Option[Double] = None
	var priorityId: String = null
	var priorityDisplayName: String = null
	var ratingModelId: String = null
	var modelSummaryId: String = null
	var launchTime: Option[Long] = None
	
    if (playLaunch.getCreated() != null) {
        launchTime = Some(playLaunch.getCreated().getTime())
    } else {
    	launchTime = Some(launchTimestampMillis)
    }
	
	val sfdcAccountId: String = if (playLaunchContext.getSfdcAccountID() == null) null else checkAndGet(account, playLaunchContext.getSfdcAccountID())
	
	var companyName: String = checkAndGet(account, InterfaceName.CompanyName.name())
	if (StringUtils.isBlank(companyName)) {
		companyName = checkAndGet(account, InterfaceName.LDC_Name.name())
	}
	
	if (playLaunchContext.getRatingId() != null) {
		score = checkAndGet(account,
                    playLaunchContext.getRatingId() + PlaymakerConstants.RatingScoreColumnSuffix)
        bucketName = checkAndGet(account, playLaunchContext.getRatingId(),
                    RatingBucketName.getUnscoredBucketName())
        if (EnumUtils.isValidEnum(classOf[RatingBucketName], bucketName)) {
        	bucket = RatingBucketName.valueOf(bucketName)
        } else {
        	bucket = null
        }
        likelihood = if (StringUtils.isNotEmpty(score)) Some(score.toDouble) else Some(getDefaultLikelihood(bucket))
        expectedValue = checkAndGet(account,
                    playLaunchContext.getRatingId() + PlaymakerConstants.RatingEVColumnSuffix)
        monetaryValue = if (StringUtils.isNotEmpty(expectedValue)) Some(expectedValue.toDouble) else None
        synchronizationDestination = playLaunchContext.getSynchronizationDestination()
        destinationOrgId = playLaunchContext.getDestinationOrgId()
        destinationSysType = playLaunchContext.getDestinationSysType()
        
        priorityId = if (bucket != null) bucket.name() else null
        priorityDisplayName = bucketName
        ratingModelId = playLaunchContext.getPublishedIteration().getId()
        if (playLaunchContext.getPlay().getRatingEngine().getType() != RatingEngineType.RULE_BASED) {
        	modelSummaryId = playLaunchContext.getPublishedIteration().asInstanceOf[AIModel].getModelSummaryId()
        } else {
        	modelSummaryId = ""
        }
	}
	
	Recommendation(None, // PID
				  uuid, // EXTERNAL_ID
				  accountId, // ACCOUNT_ID
				  externalAccountId, // LE_ACCOUNT_EXTERNAL_ID
				  playId, // PLAY_ID
				  playLaunchId, // LAUNCH_ID
				  description, // DESCRIPTION
				  launchTime, // LAUNCH_DATE
				  null, // LAST_UPDATED_TIMESTAMP
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
				  false // DELETED
				  )
  }
  
  def getDefaultLikelihood(bucket: RatingBucketName): Double = {
  	if (bucket == null) {
  		return 0.0
  	}
  	bucket match {
  		case RatingBucketName.A => return 95.0
  		case RatingBucketName.B => return 70.0
  		case RatingBucketName.C => return 40.0
  		case RatingBucketName.D => return 20.0
  		case RatingBucketName.E => return 10.0
  		case RatingBucketName.F => return 5.0
  		case _ => throw new UnsupportedOperationException("Unknown bucket " + bucket)
  	}
  }
  
   def checkAndGet(account: Row, field: String, defaultValue: String): String = {
  	try {
  		return account.getString(account.fieldIndex(field))
  	} catch {
  		case e: IllegalArgumentException => return defaultValue
  	}
  }
  
  def checkAndGet(account: Row, field: String): String = {
  	checkAndGet (account, field, null)
  }

  override def runJob(spark: SparkSession, lattice: LatticeContext[CreateRecommendationConfig]): Unit = {
    val config: CreateRecommendationConfig = lattice.config
    val playLaunchContext: PlayLaunchSparkContext = config.getPlayLaunchSparkContext
    val joinKey: String = playLaunchContext.getJoinKey
	println(s"joinKey is: $joinKey")
	
	// Read Input
    val accountTable: DataFrame = lattice.input.head
    val contactTable: DataFrame = lattice.input(1)
    
    // Manipulate Account Table with PlayLaunchContext
    val accountAndplayLaunch = accountTable.rdd.map { a => createRec(a, playLaunchContext) }
    val derivedAccounts = spark.createDataFrame(accountAndplayLaunch).toDF("PID", "EXTERNAL_ID", "ACCOUNT_ID", "LE_ACCOUNT_EXTERNAL_ID", "PLAY_ID", "LAUNCH_ID", "DESCRIPTION"
    , "LAUNCH_DATE", "LAST_UPDATED_TIMESTAMP", "MONETARY_VALUE", "LIKELIHOOD", "COMPANY_NAME", "SFDC_ACCOUNT_ID", "PRIORITY_ID", "PRIORITY_DISPLAY_NAME", "MONETARY_VALUE_ISO4217_ID", 
    "LIFT", "RATING_MODEL_ID", "MODEL_SUMMARY_ID", "SYNC_DESTINATION", "DESTINATION_ORG_ID", "DESTINATION_SYS_TYPE", "TENANT_ID", "DELETED")

	// Manipulate Contact Table
	val contactWithoutJoinKey = contactTable.drop(joinKey)
	spark.udf.register("flatten", new Flatten(contactWithoutJoinKey.schema))
	val f = new Flatten(contactWithoutJoinKey.schema)
	val aggregatedContacts = contactTable.groupBy(joinKey).agg(f(contactWithoutJoinKey.columns map col: _*).as("CONTACTS"))

    // join
    val recommendations = derivedAccounts.join(aggregatedContacts, joinKey::Nil, "left")
	
    // finish
    lattice.output = recommendations::Nil
    lattice.outputStr = "These are my recommendations!"
  }

}
