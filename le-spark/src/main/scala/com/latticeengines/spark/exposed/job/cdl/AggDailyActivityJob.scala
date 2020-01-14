package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.cdl.activity.StreamAttributeDeriver.Calculation._
import com.latticeengines.domain.exposed.cdl.activity.{DimensionCalculator, DimensionCalculatorRegexMode, DimensionGenerator}
import com.latticeengines.domain.exposed.metadata.InterfaceName.{__Row_Count__, __StreamDate, __StreamDateId}
import com.latticeengines.domain.exposed.spark.cdl.AggDailyActivityConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.json4s.jackson.Serialization

import scala.collection.JavaConverters._

class AggDailyActivityJob extends AbstractSparkJob[AggDailyActivityConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[AggDailyActivityConfig]): Unit = {
    // define vars
    val inputIdx = lattice.config.rawStreamInputIdx.asScala
    val metadataMap = lattice.config.dimensionMetadataMap.asScala.mapValues(_.asScala)
    val calculatorMap = lattice.config.dimensionCalculatorMap.asScala.mapValues(_.asScala)
    val attrDeriverMap = lattice.config.attrDeriverMap.asScala.mapValues(_.asScala.toList)
    val entityIdColMap = lattice.config.additionalDimAttrMap.asScala.mapValues(_.asScala.toList)
    val hashDimensionMap = lattice.config.hashDimensionMap.asScala.mapValues(_.asScala.toSet)
    val dimValueIdMap = lattice.config.dimensionValueIdMap.asScala
    val streamReducerMap = lattice.config.streamReducerMap.asScala

    // calculation
    val streamDfs = inputIdx
      .filterKeys(metadataMap.contains)
      .filterKeys(metadataMap(_).nonEmpty)
      .map { case (streamId, idx) =>
        val metadataInStream = metadataMap(streamId)
        val calculators = calculatorMap(streamId)
        val attrs = metadataInStream.keys
        val additionalDimCols = entityIdColMap.getOrElse(streamId, Nil)
        val hashDimensions = hashDimensionMap.getOrElse(streamId, Set())
        val valueIdMap = dimValueIdMap.clone()

        val df = attrs.foldLeft(lattice.input(idx)) { (accDf, dimensionName) =>
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
        val dimAttrs = attrs.toSeq ++ additionalDimCols :+ __StreamDate.name :+ __StreamDateId.name
        val aggFns = attrDeriverMap.getOrElse(streamId, Nil).map { deriver =>
          val col = deriver.getCalculation match {
            case COUNT => count("*")
            case SUM => sum(deriver.getSourceAttributes.get(0))
            case MAX => max(deriver.getSourceAttributes.get(0))
            case MIN => min(deriver.getSourceAttributes.get(0))
            case _ => throw new UnsupportedOperationException(s"Calculation ${deriver.getCalculation} is not supported")
          }
          col.as(deriver.getTargetAttribute)
        }

        // always generate row count agg
        val aggDf: DataFrame = {
          if (streamReducerMap.get(streamId).isDefined) {
            // if stream has reducer, just append 1 as dedup already done for daily level
            df.withColumn(__Row_Count__.name, lit(1).cast(LongType))
          } else {
            df.groupBy(dimAttrs.head, dimAttrs.tail: _*)
              .agg(count("*").as(__Row_Count__.name), aggFns: _*)
          }
        }

        (streamId, aggDf, idx)
      }

    // return aggregated dataframe sorted by input index
    val result = streamDfs.toList.sortBy(_._3)
    // all partition by dateId
    for (i <- result.indices) setPartitionTargets(i, Seq(__StreamDateId.name()), lattice)
    lattice.output = result.map(_._2)
    // json str (list of streamId with the same order as output df)
    lattice.outputStr = Serialization.write(result.map(_._1))(org.json4s.DefaultFormats)
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
      case _ =>
        // non-regex can only have one match
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
      .map(valueMap => (valueMap(ptnAttr).asInstanceOf[String]
        .replaceAll("(?<!\\.)\\*", ".*").r.pattern, valueMap))

    // can match multiple patterns
    df.flatMap(row => {
      val values = row.toSeq
      val targetStr = row.getAs[String](regexCalculator.getAttribute)
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
}
