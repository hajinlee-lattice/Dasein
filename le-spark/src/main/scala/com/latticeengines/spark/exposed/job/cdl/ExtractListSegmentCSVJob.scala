package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.template.{CSVAdaptor, ImportFieldMapping}
import com.latticeengines.domain.exposed.metadata.{InterfaceName, UserDefinedType}
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.ExtractListSegmentCSVConfiguration
import com.latticeengines.domain.exposed.spark.cdl.ExtractListSegmentCSVConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.functions.{col, lit, udf, when}
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable.{Buffer, ListBuffer}

class ExtractListSegmentCSVJob extends AbstractSparkJob[ExtractListSegmentCSVConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[ExtractListSegmentCSVConfig]): Unit = {
    val config: ExtractListSegmentCSVConfig = lattice.config
    val csvAdaptor: CSVAdaptor = config.getCsvAdaptor
    val accountAttributes: Seq[String] = config.getAccountAttributes.asScala
    val contactAttributes: Seq[String] = config.getContactAttributes.asScala
    val fieldMappings: Buffer[ImportFieldMapping] = csvAdaptor.getImportFieldMappings.asScala
    var importCSVDf: DataFrame = lattice.input.head
    val finalDfs = new ListBuffer[DataFrame]()
    val importCSVColumns: Array[String] = importCSVDf.columns
    val columnsExist: ListBuffer[String] = ListBuffer()
    val structFields: ListBuffer[StructField] = ListBuffer()
    logSpark("importCSVColumns :")
    importCSVColumns.foreach { c =>
      logSpark("column :" + c)
    }
    fieldMappings.foreach { fieldMapping =>
      if (importCSVColumns.contains(fieldMapping.getUserFieldName)) {
        columnsExist += fieldMapping.getUserFieldName
        structFields += StructField(fieldMapping.getFieldName, getFieldType(fieldMapping.getFieldType))
      }
    }
    importCSVDf = importCSVDf.select(columnsExist map col: _*)
    val outputSchema = StructType(structFields)
    val transformedInput = spark.createDataFrame(importCSVDf.rdd, outputSchema)
    finalDfs += generateEntityDf(true, transformedInput, accountAttributes)
    if (!contactAttributes.isEmpty) {
      finalDfs += generateEntityDf(false, transformedInput, contactAttributes)
    }
    lattice.output = finalDfs.toList
  }

  private def generateEntityDf(distinct: Boolean, input: DataFrame, attributes: Seq[String]): DataFrame = {
    val columnsExist: ListBuffer[String] = ListBuffer()
    val columnsNotExist: ListBuffer[String] = ListBuffer()
    attributes.foreach { attribute =>
      if (input.columns.contains(attribute)) {
        columnsExist += attribute
      } else {
        columnsNotExist += attribute
      }
    }
    var result = input.select(columnsExist map col: _*)
    columnsNotExist.map(accountColumn => result = result.withColumn(accountColumn, lit(null).cast(StringType)))
    val columns = result.columns
    val contactNameFunc: (String, String) => String = (firstName, lastName) => {
      firstName + " " + lastName
    }
    val contactNameUdf = udf(contactNameFunc)
    if (!columns.contains(InterfaceName.PhoneNumber.name())) {
      for (field <- columns) {
        if (field.equalsIgnoreCase(ExtractListSegmentCSVConfiguration.Direct_Phone)) {
          result = result.withColumnRenamed(field, InterfaceName.PhoneNumber.name)
        } else if (field.equalsIgnoreCase("ContactName")) {
          result = result.withColumn(field, when(col("FirstName").isNotNull,
            contactNameUdf(col("FirstName"), col("LastName"))).otherwise(lit(null).cast(StringType)))
        }
      }
    }
    if (distinct && result.columns.contains(InterfaceName.AccountId.name())) {
      result = result.dropDuplicates(InterfaceName.AccountId.name())
    }
    result
  }

  private def getFieldType(userType: UserDefinedType): DataType = {
    userType match {
      case null => StringType
      case UserDefinedType.BOOLEAN => BooleanType
      case UserDefinedType.DATE => LongType
      case UserDefinedType.INTEGER => IntegerType
      case UserDefinedType.NUMBER => DoubleType
      case UserDefinedType.TEXT => StringType
    }
  }

}