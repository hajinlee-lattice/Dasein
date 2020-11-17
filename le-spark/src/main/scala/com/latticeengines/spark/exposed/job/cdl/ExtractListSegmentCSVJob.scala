package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.UserDefinedType
import com.latticeengines.domain.exposed.metadata.template.{CSVAdaptor, ImportFieldMapping}
import com.latticeengines.domain.exposed.spark.cdl.ExtractListSegmentCSVConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.functions.{col, lit}
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
    var importCSVDf: DataFrame = lattice.input(1)
    val finalDfs = new ListBuffer[DataFrame]()
    val importCSVColumns: Array[String] = importCSVDf.columns
    val columnsExist: ListBuffer[String] = ListBuffer()
    val structFields: ListBuffer[StructField] = ListBuffer()
    fieldMappings.foreach { fieldMapping =>
      if (importCSVColumns.contains(fieldMapping.getFieldName)) {
        columnsExist += fieldMapping.getFieldName
        structFields += StructField(fieldMapping.getFieldName, getFieldType(fieldMapping.getFieldType))
      }
    }
    importCSVDf = importCSVDf.select(columnsExist map col: _*)
    val outputSchema = StructType(structFields)
    val transformedInput = spark.createDataFrame(importCSVDf.rdd, outputSchema)
    finalDfs += generateEntityDf(transformedInput, accountAttributes)
    finalDfs += generateEntityDf(transformedInput, contactAttributes)
    lattice.output = finalDfs.toList
  }

  private def generateEntityDf(input: DataFrame, attributes: Seq[String]): DataFrame = {
    val columnsExist: ListBuffer[String] = ListBuffer()
    val columnsNotExist: ListBuffer[String] = ListBuffer()
    input.columns.foreach { field =>
      if (attributes.contains(field)) {
        columnsExist += field
      } else {
        columnsNotExist += field
      }
    }
    val result = input.select(columnsExist map col: _*)
    columnsNotExist.map(accountColumn => result.withColumn(accountColumn, lit(null).cast(StringType)))
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