package com.latticeengines.spark.exposed.job.cdl

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util
import com.latticeengines.common.exposed.util.DateTimeUtils.{dateToDayPeriod, toDateOnlyFromMillis}
import com.latticeengines.domain.exposed.cdl.activity.StreamAttributeDeriver.Calculation._
import com.latticeengines.domain.exposed.cdl.activity._
import com.latticeengines.domain.exposed.metadata.InterfaceName.{DerivedId, LastActivityDate, StreamDateId, __Row_Count__, __StreamDate}
import com.latticeengines.domain.exposed.spark.cdl.SparkIOMetadataWrapper.Partition
import com.latticeengines.domain.exposed.spark.cdl.{AggDailyActivityConfig, SparkIOMetadataWrapper}
import com.latticeengines.domain.exposed.util.ActivityStoreUtils
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.{DeriveAttrsUtils, MergeUtils}
import org.apache.commons.collections4.CollectionUtils
import org.apache.commons.lang3.{BooleanUtils, StringUtils}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, LongType, StringType, StructField}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._

class AggDailyActivityJob extends AbstractSparkJob[AggDailyActivityConfig] {

  val partitionKey: String = StreamDateId.name

  override def runJob(spark: SparkSession, lattice: LatticeContext[AggDailyActivityConfig]): Unit = {
    val inputMetadata = lattice.config.inputMetadata.getMetadata
    var output: Seq[DataFrame] = Seq()
    val outputMetadata: SparkIOMetadataWrapper = new SparkIOMetadataWrapper()
    val detailsMap = new util.HashMap[String, Partition]() // streamId -> details
    var idx: Int = 0
    lattice.config.streamReducerMap.values().asScala.foreach((reducer: ActivityRowReducer) => {
      if (DeriveAttrsUtils.isTimeReducingOperation(reducer.getOperator) && !reducer.getGroupByFields.contains(__StreamDate.name)) {
        reducer.getGroupByFields.asScala.append(__StreamDate.name) // separate by each day if filter is applied for time
      }
    })
    inputMetadata.asScala.foreach(entry => {
      var missingBatch: Boolean = false
      val (streamId: String, details: Partition) = entry
      if (lattice.config.incrementalStreams.contains(streamId)) {
        val matchedImportDF: DataFrame = lattice.input(details.getStartIdx)
        if (CollectionUtils.isNotEmpty(details.getLabels) && details.getLabels.contains(ActivityMetricsGroupUtils.NO_BATCH)) {
          missingBatch = true
        }
        val rawStreamDelta: DataFrame = processImportToRawStream(streamId, matchedImportDF, lattice)
        val dailyStoreDelta: DataFrame = processRawStream(streamId, rawStreamDelta, lattice)
        var (updatedDailyStoreBatch: DataFrame, updatedDailyStoreDelta: DataFrame) = {
          if (missingBatch) {
            (dailyStoreDelta, dailyStoreDelta)
          } else {
            var dailyStoreBatch: DataFrame = lattice.input(details.getStartIdx + 1)
            if (lattice.config.streamRetentionDays.containsKey(streamId)) {
              dailyStoreBatch = dailyStoreBatch.filter(dailyStoreBatch(StreamDateId.name).geq(getStartDateId(lattice.config.streamRetentionDays.get(streamId), lattice.config.currentEpochMilli)))
            }
            updateDailyStoreBatchAndDelta(streamId, dailyStoreDelta, dailyStoreBatch, lattice)
          }
        }
        output :+= updatedDailyStoreDelta
        output :+= updatedDailyStoreBatch
        detailsMap.put(streamId, setDetails(idx))
        idx += 2
      } else {
        val rawStream = lattice.input(lattice.config.inputMetadata.getMetadata.get(streamId).getStartIdx)
        output :+= processRawStream(streamId, rawStream, lattice)
        detailsMap.put(streamId, setDetails(idx))
        idx += 1
      }
    })
    outputMetadata.setMetadata(detailsMap)
    for (index <- output.indices) {
      setPartitionTargets(index, Seq(partitionKey), lattice)
    }
    val result = {
      if (BooleanUtils.isNotFalse(lattice.config.repartition)) {
        output.map(_.repartition(200, col(partitionKey)))
      } else {
        output
      }
    }
    lattice.output = result.toList
    lattice.outputStr = serializeJson(outputMetadata)
  }

  // Update batch store with delta. Update delta as well if reducer exists
  // return (updatedBatch, updatedDelta)
  def updateDailyStoreBatchAndDelta(streamId: String, dailyStoreDelta: DataFrame, dailyStoreBatch: DataFrame, lattice: LatticeContext[AggDailyActivityConfig]): (DataFrame, DataFrame) = {
    val metadataMap = lattice.config.dimensionMetadataMap.asScala.mapValues(_.asScala)
    val attrDeriverMap = lattice.config.attrDeriverMap.asScala.mapValues(_.asScala.toList)
    val entityIdColMap = lattice.config.additionalDimAttrMap.asScala.mapValues(_.asScala.toList)

    val metadataInStream = metadataMap(streamId)
    val attrs = metadataInStream.keys
    val additionalDimCols = entityIdColMap.getOrElse(streamId, Nil)
    // aggregation: all dimension name + entityIds + stream date & dateId
    val dimAttrs = attrs.toSeq ++ additionalDimCols :+ __StreamDate.name :+ StreamDateId.name
    val dateIdRangeToUpdate: Array[Int] = dailyStoreDelta.select(StreamDateId.name).distinct.rdd.map(r => r(0).asInstanceOf[Int]).collect()

    val aggFns = attrDeriverMap.getOrElse(streamId, Nil).map { deriver =>
      val col = deriver.getCalculation match {
        case COUNT => count("*")
        case SUM => sum(deriver.getSourceAttributes.get(0))
        case MAX => max(deriver.getSourceAttributes.get(0))
        case MIN => min(deriver.getSourceAttributes.get(0))
        case TRUE => lit(true).cast(BooleanType)
        case _ => throw new UnsupportedOperationException(s"Calculation ${deriver.getCalculation} is not supported")
      }
      col.as(deriver.getTargetAttribute)
    } :+ max(DeriveAttrsUtils.VERSION_COL).as(DeriveAttrsUtils.VERSION_COL)
    val dailyStoreBatchWithVersion: DataFrame = {
      if (!dailyStoreBatch.columns.contains(DeriveAttrsUtils.VERSION_COL)) {
        DeriveAttrsUtils.appendVersionStamp(dailyStoreBatch, 0L)
      } else {
        dailyStoreBatch
      }
    }
    val affected: DataFrame = dailyStoreBatchWithVersion.filter(col(StreamDateId.name).isInCollection(dateIdRangeToUpdate))
    val notAffected: DataFrame = dailyStoreBatchWithVersion.filter(!col(StreamDateId.name).isInCollection(dateIdRangeToUpdate))
    val updatedDelta: DataFrame = {
      if (lattice.config.streamReducerMap.containsKey(streamId)) {
        val reducer = lattice.config.streamReducerMap.get(streamId)
        DeriveAttrsUtils.applyReducer(MergeUtils.concat2(affected, dailyStoreDelta), reducer)
      } else {
        MergeUtils.concat2(affected, dailyStoreDelta)
          .groupBy(dimAttrs.head, dimAttrs.tail: _*)
          .agg(sum(__Row_Count__.name).as(__Row_Count__.name), max(LastActivityDate.name).as(LastActivityDate.name) :: aggFns: _*)
      }
    }
    val newBatch: DataFrame = MergeUtils.concat2(notAffected, updatedDelta)
    (
      newBatch,
      updatedDelta
    )
  }

  def processImportToRawStream(streamId: String, matchedImportDF: DataFrame, lattice: LatticeContext[AggDailyActivityConfig]): DataFrame = {
    val getDate = udf {
      time: Long => toDateOnlyFromMillis(time.toString)
    }
    val getDateId = udf {
      time: Long => dateToDayPeriod(toDateOnlyFromMillis(time.toString))
    }
    val dateAttr = lattice.config.streamDateAttrs.get(streamId)
    var rawDF = matchedImportDF
    rawDF = rawDF.withColumn(__StreamDate.name, getDate(rawDF.col(dateAttr)))
      .withColumn(StreamDateId.name, getDateId(rawDF.col(dateAttr)))
      .withColumn(LastActivityDate.name, rawDF.col(dateAttr))
    if (lattice.config.streamReducerMap.containsKey(streamId)) {
      val reducer = lattice.config.streamReducerMap.get(streamId)
      DeriveAttrsUtils.applyReducer(rawDF, reducer)
    } else {
      rawDF
    }
  }

  // process a raw stream table to daily stream
  def processRawStream(streamId: String, rawStream: DataFrame, lattice: LatticeContext[AggDailyActivityConfig]): DataFrame = {
    val dateAttrs = lattice.config.streamDateAttrs.asScala
    val metadataMap = lattice.config.dimensionMetadataMap.asScala.mapValues(_.asScala)
    val calculatorMap = lattice.config.dimensionCalculatorMap.asScala.mapValues(_.asScala)
    val attrDeriverMap = lattice.config.attrDeriverMap.asScala.mapValues(_.asScala.toList)
    val entityIdColMap = lattice.config.additionalDimAttrMap.asScala.mapValues(_.asScala.toList)
    val hashDimensionMap = lattice.config.hashDimensionMap.asScala.mapValues(_.asScala.toSet)
    val dimValueIdMap = lattice.config.dimensionValueIdMap.asScala
    val streamReducerMap = lattice.config.streamReducerMap.asScala
    val version: Long = lattice.config.currentEpochMilli

    val metadataInStream = metadataMap.getOrElse(streamId, Map.empty[String, DimensionMetadata])
    val dateAttr = dateAttrs(streamId)
    val calculators = calculatorMap.getOrElse(streamId, Map.empty[String, DimensionCalculator])
    val attrs = metadataInStream.keys
    val additionalDimCols = entityIdColMap.getOrElse(streamId, Nil)
    val hashDimensions = hashDimensionMap.getOrElse(streamId, Set())
    val valueIdMap = dimValueIdMap.clone()

    val df = attrs.foldLeft(rawStream) { (accDf, dimensionName) =>
      val dimValues = metadataInStream(dimensionName).getDimensionValues.asScala.map(_.asScala.toMap).toList
      val calculator = calculators(dimensionName)
      val getValFn: AnyRef => String = (obj: AnyRef) =>
        if (hashDimensions.contains(dimensionName)) {
          valueIdMap.get(DimensionGenerator.hashDimensionValue(obj)).orNull
        } else {
          Option(obj).map(_.toString).map(valueIdMap.get(_).orNull).orNull
        }
      // generate dimension value (set to null if not match any value in DimensionMetadata)
      DimensionValueHelper.calculateDimensionValue(accDf, dimensionName, dimValues, calculator, getValFn)
    }

    // aggregation: all dimension name + entityIds + stream date & dateId
    val dimAttrs = attrs.toSeq ++ additionalDimCols :+ __StreamDate.name :+ StreamDateId.name
    val aggFns = attrDeriverMap.getOrElse(streamId, Nil).map(DeriveAttrsUtils.getAggr(df, _))

    // always generate row count agg
    val aggDf: DataFrame = {
      if (streamReducerMap.contains(streamId)) {
        // if stream has reducer, just append 1 as dedup already done for daily level
        addLastActivityDateColIfNotExist(df, dateAttr).withColumn(__Row_Count__.name, lit(1).cast(LongType))
      } else {
        addLastActivityDateColIfNotExist(df, dateAttr)
          .groupBy(dimAttrs.head, dimAttrs.tail: _*)
          .agg(count("*").as(__Row_Count__.name),
            max(LastActivityDate.name).as(LastActivityDate.name) :: aggFns: _*)
      }
    }
    if (!aggDf.columns.contains(DeriveAttrsUtils.VERSION_COL)) {
      DeriveAttrsUtils.appendVersionStamp(aggDf, version)
    } else {
      aggDf
    }
  }

  private def addLastActivityDateColIfNotExist(df: DataFrame, dateAttr: String): DataFrame = {
    if (!df.columns.contains(LastActivityDate.name)) {
      df.withColumn(LastActivityDate.name, df.col(dateAttr))
    } else {
      df.withColumn(LastActivityDate.name,
        coalesce(df.col(LastActivityDate.name), df.col(dateAttr), lit(null).cast(LongType)))
    }
  }

  def setDetails(index: Int): Partition = {
    val details = new Partition()
    details.setStartIdx(index)
    details
  }

  private def getStartDateId(retentionDays: Int, epochMilli: Long): Int = {
    val startTime = Instant.ofEpochMilli(epochMilli).minus(retentionDays, ChronoUnit.DAYS).toEpochMilli.toString
    dateToDayPeriod(toDateOnlyFromMillis(startTime))
  }
}

/*
 * helper methods for dimension value calculation
 */
object DimensionValueHelper extends Serializable {

  def calculateDimensionValue(df: DataFrame, dimensionName: String, dimValues: List[Map[String, AnyRef]],
                              calculator: DimensionCalculator,
                              getValueFn: AnyRef => String): DataFrame = {
    calculator match {
      case regexCalculator: DimensionCalculatorRegexMode =>
        DimensionValueHelper.matchRegexDimensionValue(df, dimensionName, dimValues, regexCalculator, getValueFn)
      case compositeDimensionCalculator: CompositeDimensionCalculator =>
        DimensionValueHelper.deriveCompositeDimensionValue(df, compositeDimensionCalculator.deriveConfig, getValueFn)
      case _ =>
        // non-regex can only have one match (exact match)
        val calDimValue = udf {
          obj: AnyRef =>
            dimValues
              .map(_ (dimensionName).toString)
              .find(_.equals(getValueFn(obj)))
              .orNull
        }
        df.withColumn(dimensionName, calDimValue(df.col(calculator.getAttribute)))
    }
  }

  def matchRegexDimensionValue(df: DataFrame, dimensionName: String, dimValues: List[Map[String, AnyRef]],
                               regexCalculator: DimensionCalculatorRegexMode,
                               getValueFn: AnyRef => String): DataFrame = {
    val ptnAttr = regexCalculator.getPatternAttribute
    // seq of (pattern, valueMap)
    /*-
     * NOTE to support wildcard, replace all * with .*
     *      this will break regex usage with asterisk
     * NOTE use negative lookbehind to exclude replacing existing .*
     */
    val metadataWithPtn = dimValues
      .map(valueMap => (ActivityStoreUtils.modifyPattern(valueMap(ptnAttr).asInstanceOf[String]).r.pattern, valueMap))

    val fieldIndexMap = df.schema.fieldNames.zipWithIndex.toMap

    // can match multiple patterns
    df.flatMap(row => {
      val values = row.toSeq
      val targetStr = row.getAs[String](fieldIndexMap(regexCalculator.getAttribute))
      if (StringUtils.isBlank(targetStr)) {
        Row.fromSeq(values :+ null) :: Nil
      } else {
        val rows = metadataWithPtn
          .filter(_._1.matcher(targetStr).matches)
          .map(_._2(dimensionName).toString)
          .map(values :+ _) // add dimension attr value to the end
          .map(Row.fromSeq)
        if (rows.isEmpty) {
          // no match
          Row.fromSeq(values :+ null) :: Nil
        } else {
          rows
        }
      }
    })(RowEncoder(df.schema.add(StructField(dimensionName, StringType, nullable = true))))
  }

  def deriveCompositeDimensionValue(df: DataFrame, deriveConfig: DeriveConfig, getValueFn: AnyRef => String): DataFrame = {
    val deriveSrc = deriveConfig.sourceAttrs.asScala
    val getDimName: UserDefinedFunction = udf((row: Row) => {
      val vals: List[String] = deriveSrc.map(src => {
        val value = row.getAs[String](src)
        if (StringUtils.isBlank(value)) "" else value
      }).toList
      deriveConfig.findDimensionName(vals.asJava)
    })
    val columnNames = df.columns
    // hash of derived dimension name value
    val withRawId: DataFrame = df.withColumn("__raw_id", getDimName(struct(columnNames.map(colName => col(colName)): _*)))
    val getDerivedIdUdf: UserDefinedFunction = udf((rawId: String) => getValueFn(rawId))
    withRawId.withColumn(DerivedId.name, getDerivedIdUdf(col("__raw_id"))).drop("__raw_id")
  }
}
