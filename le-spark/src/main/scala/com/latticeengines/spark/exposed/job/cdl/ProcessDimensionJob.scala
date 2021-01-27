package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.cdl.activity.DimensionGenerator
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata.{CARDINALITY_KEY, DIMENSION_VALUES_KEY}
import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.spark.cdl.ProcessDimensionConfig
import com.latticeengines.domain.exposed.spark.cdl.ProcessDimensionConfig.DerivedDimension
import com.latticeengines.spark.exposed.job.cdl.ProcessDimensionJob.FREQ_COL
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.json4s.jackson.Serialization

import scala.collection.JavaConverters._

/**
 * Go through dimension value source (Stream or Catalog) and form its value space
 */
class ProcessDimensionJob extends AbstractSparkJob[ProcessDimensionConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[ProcessDimensionConfig]): Unit = {
    val dimensions = lattice.config.dimensions.asScala

    val dimDfs = dimensions mapValues { dim =>
      val attrs = dim.attrs.asScala.toSeq
      val dedupAttrs = if (dim.dedupAttrs == null) attrs else dim.dedupAttrs.asScala.toSeq
      val renameAttrs = Option(dim.renameAttrs)
      val hashVal: UserDefinedFunction = udf { obj: Any => DimensionGenerator.hashDimensionValue(obj) }

      val allAttrNotNull = attrs.foldLeft(lit(true)) {
        (acc, attr) => acc.and(col(attr).isNotNull)
      }

      // hash & rename
      val inputDf = lattice.input(dim.inputIdx)
      var df = hashDimension(inputDf, dim, hashVal)
      df = renameAttrs.fold(df)(_.asScala.foldLeft(df) {
        (accDf, entry) => accDf.withColumnRenamed(entry._2, entry._1)
      })

      // group by and order by frequency
      df = df.filter(allAttrNotNull)
        .groupBy(attrs.head, attrs.tail: _*)
        .agg(count("*").as(FREQ_COL))
        /*-
         * randomly pick one, no guarantee even if after orderBy
         * TODO keep the max freq in each group
         */
        .dropDuplicates(dedupAttrs)
        .orderBy(desc(FREQ_COL))
        .drop(FREQ_COL)
      if (dim.valueLimit != null) {
        (df.limit(dim.valueLimit), df.count)
      } else {
        (df, df.count)
      }
    }

    // (dimId, metadata dataframe, cardinality)
    val dimResults = dimDfs.map(t => (t._1, t._2._1, t._2._2)).toList
    if (lattice.config.collectMetadata) {
      val dimensionMetadata = dimResults.map { case (dimId, df, cardinality) =>
        val values = df.collect
          .map(row => row.getValuesMap[Any](row.schema.fieldNames))
          .toList
        (dimId, Map(DIMENSION_VALUES_KEY -> values, CARDINALITY_KEY -> cardinality))
      }.toMap
      lattice.outputStr = Serialization.write(dimensionMetadata)(org.json4s.DefaultFormats)
    } else {
      val outputIdxMap = dimResults.zipWithIndex.map(t => (t._1._1, t._2)).toMap
      lattice.output = dimResults.map(_._2)
      lattice.outputStr = Serialization.write(outputIdxMap)(org.json4s.DefaultFormats)
    }
  }

  def hashDimension(inputDf: DataFrame, dim: ProcessDimensionConfig.Dimension, hashVal: UserDefinedFunction): DataFrame = {
    dim match {
      case derivedDimension: DerivedDimension =>
        val dimIdName: String = InterfaceName.DerivedId.name
        val dimName: String = InterfaceName.DerivedName.name
        val dimPattern: String = InterfaceName.DerivedPattern.name
        val deriveConfig = derivedDimension.deriveConfig
        val deriveSrc = deriveConfig.sourceAttrs.asScala
        val getDimNameUdf: UserDefinedFunction = udf((row: Row) => {
          val vals: List[String] = deriveSrc.map(src => {
            val value = row.getAs[String](src)
            if (StringUtils.isBlank(value)) "" else value
          }).toList
          deriveConfig.findDimensionName(vals.asJava)
        })
        val getDimIdUdf: UserDefinedFunction = udf((row: Row) => {
          val vals: List[String] = deriveSrc.map(src => {
            val value = row.getAs[String](src)
            if (StringUtils.isBlank(value)) "" else value
          }).toList
          deriveConfig.findDimensionId(vals.asJava)
        })
        val getDimPatternUdf: UserDefinedFunction = udf((row: Row) => {
          val vals: List[String] = deriveSrc.map(src => {
            val value = row.getAs[String](src)
            if (StringUtils.isBlank(value)) "" else value
          }).toList
          deriveConfig.findDimensionPattern(vals.asJava)
        })
        val columnNames = inputDf.columns
        inputDf.withColumn(dimIdName, getDimIdUdf(struct(columnNames.map(colName => col(colName)): _*)))
          .withColumn(dimName, getDimNameUdf(struct(columnNames.map(colName => col(colName)): _*)))
          .withColumn(dimPattern, getDimPatternUdf(struct(columnNames.map(colName => col(colName)): _*)))
      case _ => Option(dim.hashAttrs).fold(inputDf)(_.asScala.foldLeft(inputDf) {
        (accDf, entry) => accDf.withColumn(entry._2, hashVal(col(entry._1)))
      })
    }
  }
}

object ProcessDimensionJob {
  private val FREQ_COL = "__dim_frequency"
}
