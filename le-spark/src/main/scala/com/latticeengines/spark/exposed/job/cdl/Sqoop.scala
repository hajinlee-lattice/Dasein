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


class Sqoop extends AbstractSparkJob[CreateRecommendationConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[CreateRecommendationConfig]): Unit = {
    // Manipulate Account Table with PlayLaunchContext
   
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
                 2.0, // 17
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
      StructField("LIFT", DoubleType, nullable = false),
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
