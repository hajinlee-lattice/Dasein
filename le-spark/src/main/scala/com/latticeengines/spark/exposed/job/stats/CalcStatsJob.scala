package com.latticeengines.spark.exposed.job.stats

import com.latticeengines.common.exposed.util.JsonUtils
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants._
import com.latticeengines.domain.exposed.datacloud.dataflow._
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook
import com.latticeengines.domain.exposed.spark.stats.CalcStatsConfig
import com.latticeengines.spark.aggregation.BucketCountAggregation
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.{BitEncodeUtils, DimensionUtils}
import org.apache.commons.collections4.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._
import scala.collection.mutable

class CalcStatsJob extends AbstractSparkJob[CalcStatsConfig] {

  private val AttrName = STATS_ATTR_NAME
  private val SrcAttrName = PROFILE_ATTR_SRCATTR
  private val BktAlgo = PROFILE_ATTR_BKTALGO
  private val Bkts = STATS_ATTR_BKTS
  private val NotNullCount = STATS_ATTR_COUNT
  private val Value = "Value"
  private val BktId = "BktId"
  private val BktSum = "BktSum"
  private val Count = "Count"
  private val HasBkts = "HasBkts"
  private val ALL = "__ALL__"

  override def runJob(spark: SparkSession, lattice: LatticeContext[CalcStatsConfig]): Unit = {
    val inputData: DataFrame = lattice.input.head
    val profileData: DataFrame = lattice.input(1)
    val config: CalcStatsConfig = lattice.config

    val profileAttrs: Seq[String] = profileData.select(AttrName).collect().map(r => r.getString(0))
    val expanded = expandEncodedByAM(inputData, profileAttrs, config)
    val filteredProfile = filterProfile(profileData)

    val rollupPaths = if (config.getDimensionTree == null) Seq()
    else getRollupPaths(config.getDimensionTree.asScala.map(t => {
      val lst = if (t._2 == null) List() else t._2.asScala.toList
      (t._1, lst)
    }).toMap)

    val rollupAttrs: Set[String] =
      if (config.getDimensionTree == null) Set()
      else config.getDimensionTree.keySet().asScala.toSet
    val rollupFields = expanded.schema.toList.filter(f => rollupAttrs.contains(f.name))
    rollupFields.foreach(f => {
      if (!StringType.equals(f.dataType)) {
        throw new UnsupportedOperationException(s"Cannot support non-string roll up field ${f.name}")
      }
    })

    val dedupAttrs: Set[String] =
      if (config.getDedupFields == null) Set()
      else config.getDedupFields.asScala.toSet
    val dedupFields = expanded.schema.toList.filter(f => dedupAttrs.contains(f.name))
    val dimFields = rollupFields union dedupFields
    val depivotedMap = depivot(expanded, dimFields)

    val withBktId = depivotedMap.values.map(df => {
      val withBktId = toBktId(df, filteredProfile)
      if (dimFields.isEmpty) {
        withBktId.groupBy(withBktId.columns map col: _*).count().withColumnRenamed("count", Count)
      } else {
        rollupBktId(withBktId, rollupPaths, dedupAttrs)
      }
    }).reduce(_ union _).persist(StorageLevel.DISK_ONLY)

    val bktCntAgg = new BucketCountAggregation
    val grpCols = withBktId.columns.diff(Seq(BktId, Count))
    val withBktCnt = withBktId.join(filteredProfile, Seq(AttrName), joinType = "left")
            .groupBy(grpCols map col: _*)
            .agg(bktCntAgg(col(BktId), col(Count)).as("agg"))
            .withColumn(NotNullCount, col(s"agg.$BktSum"))
            .withColumn(Bkts, col(s"agg.$Bkts"))
            .drop("agg")
    val rhs = filteredProfile.select(AttrName, BktAlgo)
    val result = withBktCnt.join(rhs, Seq(AttrName), joinType = "left")

    lattice.output = result :: Nil
  }

  // when running bucket encode on AM, there are big string attributes encoded by AM
  private def expandEncodedByAM(input: DataFrame, cols: Seq[String], config: CalcStatsConfig): DataFrame = {
    val codeBookLookup: Map[String, String] = //
      if (config.getCodeBookLookup == null) Map() else config.getCodeBookLookup.asScala.toMap
    val codeBookMap: Map[String, BitCodeBook] = //
      if (config.getCodeBookMap == null) Map() else config.getCodeBookMap.asScala.toMap
    if (codeBookLookup.nonEmpty && codeBookMap.nonEmpty) {
      BitEncodeUtils.decode(input, cols, codeBookLookup, codeBookMap)
    } else {
      input
    }
  }

  private def depivot(df: DataFrame, dims: List[StructField]): Map[DataType, DataFrame] = {
    val schemaMap = df.schema.groupBy(field => field.dataType)
    schemaMap.map(t => {
      val (dType, fields) = t
      val attrs: List[String] = fields.map(_.name).toList
      val depivoted = depivotCols(df, attrs, dims, dType).persist(StorageLevel.DISK_ONLY).checkpoint(eager = true)
      (dType, depivoted)
    })
  }

  private def depivotCols(df: DataFrame, attrs: Seq[String], dims: List[StructField], dataType: DataType): DataFrame = {
    val outputFields = StructField(AttrName, StringType, nullable = false) ::
            StructField(Value, dataType, nullable = true) :: dims
    val selectAttrs = attrs.union(dims.map(_.name))
    df.select(selectAttrs map col: _*).flatMap(row => {
      attrs.map(attr => {
        val dimVals: List[Any] = dims.map(dim => row.getAs[Any](dim.name))
        val attrVal: Any = if (StringType.equals(dataType)) {
          if (StringUtils.isBlank(row.getAs[String](attr))) {
            null
          } else {
            row.getAs[String](attr)
          }
        } else {
          row.getAs[Any](attr)
        }
        val values: Seq[Any] = attr :: attrVal :: dimVals
        Row.fromSeq(values)
      })
    })(RowEncoder(StructType(outputFields)))
  }

  private def filterProfile(profile: DataFrame): DataFrame = {
    val shouldBkt = udf[Boolean, String](algoStr => {
      if (StringUtils.isBlank(algoStr)) {
        false
      } else {
        val algo = JsonUtils.deserialize(algoStr, classOf[BucketAlgorithm])
        algo match {
          case bkt: DiscreteBucket =>
            CollectionUtils.isNotEmpty(bkt.getValues)
          case bkt: CategoricalBucket =>
            CollectionUtils.isNotEmpty(bkt.getCategories)
          case bkt: IntervalBucket =>
            CollectionUtils.isNotEmpty(bkt.getBoundaries)
          case _ =>
            true
        }
      }
    })
    profile.filter(shouldBkt(col(BktAlgo)))
            .withColumn(AttrName, coalesce(col(SrcAttrName), col(AttrName)))
            .select(AttrName, BktAlgo)
            .orderBy(AttrName)
            .persist(StorageLevel.MEMORY_ONLY)
  }

  private def toBktId(depivoted: DataFrame, profile: DataFrame): DataFrame = {
    val algoMap = profile.collect().map(row => {
      (row.getString(0), JsonUtils.deserialize(row.getString(1), classOf[BucketAlgorithm]))
    }).toMap
    val fields = StructField(BktId, IntegerType, nullable = false) ::
            depivoted.schema.fields.filter(f => !f.name.equals(Value)).toList
    val outputSchema = StructType(fields)
    val nameIdx = depivoted.columns.indexOf(AttrName)
    val valIdx = depivoted.columns.indexOf(Value)
    depivoted.map(row => {
      val value = row.get(valIdx)
      val attrName = row.getString(nameIdx)
      val bktId = if (value == null) {
        0
      } else if (algoMap.contains(attrName)) {
        val algo = algoMap(attrName)
        algo match {
          case bkt: DiscreteBucket =>
            val dVal = value.asInstanceOf[Number].doubleValue
            val catList = bkt.getValues.asScala.map(_.doubleValue)
            val idx = catList.indexWhere(cat => dVal == cat)
            if (idx >= 0) {
              idx + 1
            } else {
              val msg = catList.map(cat => s"$dVal == $cat: ${dVal == cat}").mkString(",")
              throw new RuntimeException(s"Cannot find value $value in given discrete list ${bkt.getValues}: $msg")
            }
          case bkt: CategoricalBucket =>
            val catList = bkt.getCategories.asScala.map(_.toLowerCase).toList
            val strV = value match {
              case s: String => s.toLowerCase
              case _ => s"$value".toLowerCase
            }
            catList.indexOf(strV) + 1
          case bkt: IntervalBucket =>
            val dVal = value.asInstanceOf[Number].doubleValue
            val bnds = bkt.getBoundaries.asScala
            bnds.foldLeft(1)((idx, bnd) => {
              if (dVal >= bnd.doubleValue) {
                idx + 1
              } else {
                idx
              }
            })
          case bkt: BooleanBucket =>
            val strV = value match {
              case s: String => s.toLowerCase
              case _ => value.toString.toLowerCase
            }
            if ("true".equals(strV) || "yes".equals(strV) || "t".equals(strV) || "y".equals(strV) || "1".equals(strV)) {
              1
            } else if ("false".equals(strV) || "no".equals(strV) || "f".equals(strV) || "n".equals(strV) || "0".equals(strV)) {
              2
            } else {
              throw new IllegalArgumentException(s"Cannot convert value $value to boolean")
            }
          case bkt: DateBucket =>
            // Decide which interval the date value falls into among the default date buckets.
            // Here are the options:
            // BUCKET #  BUCKET NAME    CRITERIA
            // 0         null           date value null, unparsable or negative
            // 1         LAST 7 DAYS    date between current time and 6 days before current time (inclusive)
            // 2         LAST 30 DAYS   date between current time and 29 days before current time (inclusive)
            // 3         LAST 90 DAYS   date between current time and 89 days before current time (inclusive)
            // 4         LAST 180 DAYS  date between current time and 179 days before current time (inclusive)
            // 5         EVER           date either after current time (in the future) or before 179 days ago
            val timeStamp: Long = value match {
              case l: Long => l
              case _ => s"$value".toLong
            }
            bkt.getDateBoundaries.asScala.foldLeft(1)((idx, bnd) => {
              if (timeStamp < bnd) {
                idx + 1
              } else {
                idx
              }
            })
          case _ =>
            throw new UnsupportedOperationException(s"Unknown algorithm ${algo.getClass}")
        }
      } else {
        1
      }
      val vals: Seq[Any] = bktId :: (0 until row.length).filter(i => !i.equals(valIdx)).map(row.get).toList
      Row.fromSeq(vals)
    })(RowEncoder(outputSchema))
  }

  private def rollupBktId(base: DataFrame, paths: Seq[Seq[AttrDimension]], dedupAttrs: Set[String]): DataFrame = {
    val withCount = if (dedupAttrs.isEmpty) {
      base.groupBy(base.columns map col: _*).count().withColumnRenamed("count", Count)
    } else {
      base.distinct()
    }
    val first = if (dedupAttrs.isEmpty) {
      withCount
    } else {
      withCount.drop(dedupAttrs.toSeq: _*).withColumn(Count, lit(1L))
    }
    val pathCache: mutable.Map[String, DataFrame] = new mutable.HashMap[String, DataFrame]()
    pathCache.put("base", withCount)
    paths.foldLeft(first)((prev, path) => {
      val parentId = DimensionUtils.getParentId(path)
      val parent = pathCache(parentId)
      val childId = DimensionUtils.pathToString(path)
      val rollupDim = path.reverse.head.getName
      val groupByAttrs = base.columns.diff(Seq(rollupDim))
      val (childBase, child) = if (dedupAttrs.isEmpty) {
        val child = parent //
                .groupBy(groupByAttrs map col: _*).agg(sum(col(Count)).as(Count)) //
                .withColumn(rollupDim, lit(ALL).cast(StringType))
        (child, child)
      } else {
        val removeRollUp = parent.drop(rollupDim).distinct()
        val childBase = removeRollUp.withColumn(rollupDim, lit(ALL).cast(StringType))
        val nonDedupGroupBy = groupByAttrs.diff(dedupAttrs.toSeq).toList
        val child = removeRollUp //
                .groupBy(nonDedupGroupBy map col: _*).count().withColumnRenamed("count", Count) //
                .withColumn(rollupDim, lit(ALL).cast(StringType))
        (childBase, child)
      }
      pathCache.put(childId, childBase)
      prev unionByName child
    })
  }

  private def getRollupPaths(dimConfig: Map[String, List[String]]): Seq[Seq[AttrDimension]] = {
    val attrDimMap = dimConfig.keys.map(a => (a, new AttrDimension(a))).toMap
    dimConfig.foreach(t => {
      val (attr, children) = t
      val dim = attrDimMap(attr)
      if (CollectionUtils.isNotEmpty(children.asJava)) {
        dim.setChildren(children.map(attrDimMap(_)).asJavaCollection)
      }
    })
    val dimensions = attrDimMap.values.toList
    DimensionUtils.getRollupPaths(dimensions)
  }

}
