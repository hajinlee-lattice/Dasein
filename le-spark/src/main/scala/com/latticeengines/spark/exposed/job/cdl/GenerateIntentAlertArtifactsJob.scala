package com.latticeengines.spark.exposed.job.cdl

import java.util

import com.latticeengines.common.exposed.util.DateTimeUtils.toDateOnlyFromMillis
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroupUtils
import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.spark.cdl.GenerateIntentAlertArtifactsConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, min, udf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class GenerateIntentAlertArtifactsJob extends AbstractSparkJob[GenerateIntentAlertArtifactsConfig] {

  private val TIMERANGE_LAST_WEEK = "w_1_w"

  private val NEW_ACCOUNTS_ROW_LIMIT = 50000
  private val ALL_ACCOUNTS_ROW_LIMIT = 200000

  override def runJob(spark: SparkSession, lattice: LatticeContext[GenerateIntentAlertArtifactsConfig]): Unit = {
    val config: GenerateIntentAlertArtifactsConfig = lattice.config
    val input: Seq[DataFrame] = lattice.input
    val dimensionValues: List[util.Map[String, AnyRef]] = config.getDimensionMetadata.getDimensionValues.asScala.toList
    val outputColumns: List[String] = config.getOutputColumns.asScala.toList
    val (latticeAccountTbl, rawstreamTble, ibmTbl, bsbmTbl) = (input(0), input(1), input(2), input(3))

    var modelNameMap: Map[String, String] = Map();
    // Generate the <modelNameId, modelName> map
    dimensionValues.foreach(map => {
      val modelNameId = map.get(InterfaceName.ModelNameId.name()).toString
      val modelName = map.get(InterfaceName.ModelName.name()).toString
      modelNameMap += (modelNameId -> modelName)
    })

    val bsbmOutputSchema = StructType(List(
      StructField(InterfaceName.AccountId.name(), StringType, nullable = true),
      StructField(InterfaceName.ModelName.name(), StringType, nullable = true),
      StructField("Stage", StringType, nullable = true)
    ))
    val bsbmConverted: DataFrame = convert(bsbmTbl, modelNameMap, bsbmOutputSchema, null)

    // Find new account that show intent in current week (latest data load) and not last week
    var ibmCols = ibmTbl.columns
      .filter(col => col != InterfaceName.AccountId.name())
      .filter(col => {
        val tokens = ActivityMetricsGroupUtils.parseAttrName(col)
        val timeRangeStr = tokens.get(2)
        timeRangeStr == TIMERANGE_LAST_WEEK
      })
      .toList
    ibmCols ++= List(InterfaceName.AccountId.name())

    val ibmSelected = ibmTbl.select(ibmCols map col: _*)
    val outputSchema = StructType(List(
      StructField(InterfaceName.AccountId.name(), StringType, nullable = true),
      StructField(InterfaceName.ModelName.name(), StringType, nullable = true)
    ))
    val ibmConverted: DataFrame = convert(ibmSelected, modelNameMap, outputSchema, false)
    val newaccounts = bsbmConverted.join(ibmConverted, Seq(InterfaceName.AccountId.name()), "left_anti")
    val newaccountsJoined = latticeAccountTbl.join(newaccounts, Seq(InterfaceName.AccountId.name()), "inner")
    val output1: DataFrame = newaccountsJoined.select(outputColumns map col: _*).limit(NEW_ACCOUNTS_ROW_LIMIT)

    // Find all accounts that show intent in current week
    var outputCols = new ListBuffer[String]()
    if (!outputColumns.contains(InterfaceName.AccountId.name())) {
      outputCols += InterfaceName.AccountId.name()
    }
    outputCols ++= outputColumns
    val joinLatticeaccount: DataFrame = bsbmConverted
      .join(latticeAccountTbl, Seq(InterfaceName.AccountId.name()), "inner")
      .select(outputCols.head, outputCols.tail: _*)
    outputCols ++= List(InterfaceName.LastModifiedDate.name())
    val joinRaw: DataFrame = joinLatticeaccount
      .join(rawstreamTble.drop(InterfaceName.ModelName.name()), Seq(InterfaceName.AccountId.name()), "inner")
      .select(outputCols.head, outputCols.tail: _*)
    outputCols -= InterfaceName.LastModifiedDate.name()
    val grouped = joinRaw
      .groupBy(outputCols.head, outputCols.tail: _*)
      .agg(min(InterfaceName.LastModifiedDate.name()).as("EarliestDate"))
    // Convert epoch time to actual date
    val getDate = udf {
      time: Long => toDateOnlyFromMillis(time)
    }
    outputCols += "Date"
    val output2: DataFrame = grouped
      .withColumn("Date", getDate(col("EarliestDate")))
      .drop("EarliestDate")
      .select(outputCols.head, outputCols.tail: _*)
      .limit(ALL_ACCOUNTS_ROW_LIMIT)

    lattice.output = output1 :: output2 :: Nil
  }

  def convert(df: DataFrame, modelNameMap: Map[String, String], outputSchema: StructType, condition: Any): DataFrame = {
    val cols = df.columns
    df.flatMap(row => {
      cols.filter(col => col != InterfaceName.AccountId.name() && row.getAs(col) != condition)
        .map(col => {
          val tokens = ActivityMetricsGroupUtils.parseAttrName(col)
          val modelNameId = tokens.get(1)
          val values: Seq[Any] = outputSchema.fieldNames map (attr => {
            if (attr == InterfaceName.AccountId.name()) {
              row.getAs(InterfaceName.AccountId.name())
            } else if (attr == InterfaceName.ModelName.name()) {
              modelNameMap(modelNameId)
            } else {
              row.getAs(col)
            }
          })
          Row.fromSeq(values)
        })
    })(RowEncoder(outputSchema))
  }
}
