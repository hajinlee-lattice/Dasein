package com.latticeengines.spark.exposed.job.cm

import java.util.UUID

import com.latticeengines.domain.exposed.datacloud.contactmaster.ContactMasterConstants
import com.latticeengines.domain.exposed.spark.cm.CMTpsSourceCreationConfig
import com.latticeengines.domain.exposed.spark.cm.CMTpsSourceCreationConfig.FieldMapping
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.JobFunctionLevelLookupUtils
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

class CreateCMTpsSourceJob extends AbstractSparkJob[CMTpsSourceCreationConfig] {
  override def runJob(spark: SparkSession, lattice: LatticeContext[CMTpsSourceCreationConfig]): Unit = {
    val config: CMTpsSourceCreationConfig = lattice.config
    val tpsSource: DataFrame = lattice.input(0)
    val fieldMapList: List[FieldMapping] = config.getFieldMaps.asScala.toList

    val getStandardJobFunctionUdf = udf((func: String) => JobFunctionLevelLookupUtils.getStandardJobFunction(func))
    val getLevelFromTitleAndFunctionUdf = udf((title: String, func: String) => JobFunctionLevelLookupUtils.getLevelFromTitleAndFunction(title, func))
    var standardized: DataFrame = tpsSource
    // Loop through the filedmapping list to add new standardized columns
    fieldMapList.foreach { fieldMapping =>
      val newField = fieldMapping.getNewStandardField
      val sourceFields = fieldMapping.getSourceFields.asScala.toList
      if (newField.toLowerCase.contains("level")) {
        standardized = standardized.withColumn(ContactMasterConstants.TPS_STANDARD_JOB_LEVEL, getLevelFromTitleAndFunctionUdf(col(sourceFields(0)), col(sourceFields(1))))
      }
      if (newField.toLowerCase.contains("function")) {
        standardized = standardized.withColumn(ContactMasterConstants.TPS_STANDARD_JOB_FUNCTION, getStandardJobFunctionUdf(col(sourceFields(0))))
      }
    }

    // Add UUID for each row since record_id won't be unique in raw data
    val generateUUID = udf(() => UUID.randomUUID().toString)
    val result = standardized.withColumn(ContactMasterConstants.TPS_RECORD_UUID, generateUUID())
    lattice.output = result :: Nil
  }
}
