package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.spark.cdl.CreateRecommendationConfig
import com.latticeengines.domain.exposed.pls.PlayLaunchSparkContext
import com.latticeengines.domain.exposed.pls.Play
import com.latticeengines.domain.exposed.pls.PlayLaunch
import com.latticeengines.domain.exposed.security.Tenant
import com.latticeengines.domain.exposed.metadata.InterfaceName

import java.util.UUID
import java.util.Date;

import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

class JoinJob extends AbstractSparkJob[CreateRecommendationConfig] {

	case class Recommendation(PID: Long, //
	                          EXTERNAL_ID: String, //
	                          ACCOUNT_I: String, //
	                          LE_ACCOUNT_EXTERNAL_ID: String, //
	                          PLAY_ID: String, //
	                          LAUNCH_ID: String, //
	                          DESCRIPTION: String, //
	                          LAUNCH_DATE: Long, //
	                          LAST_UPDATED_TIMESTAMP: Long, //
	                          MONETARY_VALUE: Double, //
	                          LIKELIHOOD: Double, //
	                          COMPANY_NAME: Double, //
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

  def createRec(account: Row, playLaunchContext: PlayLaunchSparkContext): Recommendation = {
  
    val playLaunch: PlayLaunch = playLaunchContext.getPlayLaunch
	val launchTimestampMillis: Long = playLaunchContext.getLaunchTimestampMillis
	val playName: String = playLaunchContext.getPlayName
	val playLaunchId: String = playLaunchContext.getPlayLaunchId
	val tenant: Tenant = playLaunchContext.getTenant
	
	val accountId :String = a.getString(a.fieldIndex(InterfaceName.AccountId.name()))
	val uuid: String = UUID.randomUUID().toString()
	val description: String = playLaunch.getPlay().getDescription()
	
	val launchTime: Long
    if (playLaunch.getCreated() != null) {
        launchTime = playLaunch.getCreated().getTime();
    } else {
    	launchTime = launchTimestampMillis;
    }
	
	Recommendation(hi = "hahaha" + bbb, account_id = account.account_id)
  }

  override def runJob(spark: SparkSession, lattice: LatticeContext[CreateRecommendationConfig]): Unit = {
    val config: CreateRecommendationConfig = lattice.config
    val playLaunchContext: PlayLaunchSparkContext = config.getPlayLaunchSparkContext
    val joinKey: String = playLaunchContext.getJoinKey
	println(s"joinKey is: $joinKey");
	
	// read input
    val accountTable: DataFrame = lattice.input.head
    val contactTable: DataFrame = lattice.input(1)
    
    // TODO change the Recommendation schema
    val derivedAccount = accountTable.rdd.map { a => createRec(a, playLaunchContext) }.toDF("AccountId", "destinationAccountId").show()

	// manupulate contact table
	spark.udf.register("flatten", new Flatten)
	val f = new Flatten
	val result = contactTable.groupBy("Field1").agg(f(col("ID"), col("Field2")).as("result"))
	result.show()

    // join
    val df = accountTable.join(contactTable, joinKey::Nil, "left").groupBy(joinKey)
    val out1 = df.count().withColumnRenamed("count", "Cnt")
	
    // finish
    lattice.output = out1::Nil
    lattice.outputStr = "This is my recommendation!"
  }

}
