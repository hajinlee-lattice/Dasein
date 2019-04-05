package com.latticeengines.spark.exposed.job.common

import com.latticeengines.domain.exposed.spark.common.UpsertConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.commons.collections4.CollectionUtils
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._

class UpsertJob extends AbstractSparkJob[UpsertConfig] {

  override val name = "UpsertJob"

  override def runJob(spark: SparkSession, lattice: LatticeContext[UpsertConfig]): Unit = {
    val config: UpsertConfig = lattice.config
    val inputDfs = lattice.input

    val joinKey = config.getJoinKey
    val colsFromLhs: Set[String] = if (config.getColsFromLhs == null) Set() else config.getColsFromLhs.asScala.toSet
    val lhsIdx: Int = if (config.getLhsIdx == null) 0 else config.getLhsIdx.toInt
    val rhsSeq: List[Int] =
      if (CollectionUtils.isEmpty(config.getRhsSeq))
        getDefaultRhsSeq(inputDfs.size, lhsIdx)
      else
        config.getRhsSeq.asScala.map(_.toInt).toList

    val idxSeq = lhsIdx :: rhsSeq
    val dfSeq = idxSeq map {
      inputDfs(_)
    }

    val result = (dfSeq zip idxSeq).reduce((l, r) => {
      val lhsDf = l._1
      val rhsDf = r._1
      val rhsIdx = r._2
      val joinKeysForThisJoin = Seq(joinKey)
      val colsFromLhsForThisJoin = colsFromLhs
      val merged = merge2(lhsDf, rhsDf, joinKeysForThisJoin, colsFromLhsForThisJoin)
      (merged, rhsIdx)
    })._1

    // finish
    lattice.output = result :: Nil
  }

  private def merge2(lhs: DataFrame, rhs: DataFrame, joinKeys: Seq[String], colsFromLhs: Set[String]): DataFrame = {
    val intersectCols = lhs.columns.intersect(rhs.columns).diff(joinKeys)
    val uniqueColsFromLhs = lhs.columns.diff(joinKeys.union(intersectCols))
    val uniqueColsFromRhs = rhs.columns.diff(joinKeys.union(intersectCols))
    val uniqueCols = (joinKeys union uniqueColsFromLhs union uniqueColsFromRhs).toSet

    val outputSchema =
      if (uniqueColsFromRhs.length > 0) {
        val uniqueRhs = rhs.select(uniqueColsFromRhs map col: _*)
        StructType(lhs.schema union uniqueRhs.schema)
      } else {
        lhs.schema
      }

    val lhsMarker = "__merge_marker_lhs__"
    val lhsWithMarker = lhs.withColumn(lhsMarker, lit(true))
    val rhsMarker = "__merge_marker_rhs__"
    val rhsWithMarker = rhs.withColumn(rhsMarker, lit(true))

    val join = lhsWithMarker.join(rhsWithMarker, joinKeys, "outer")
    val lhsMarkerPos = join.columns.indexOf(lhsMarker)
    val (lhsCols, rhsCols) = join.columns.splitAt(lhsMarkerPos + 1)
    val lhsColPos = lhsCols.view.zipWithIndex.toMap
    val rhsColPos = rhsCols.view.zipWithIndex.toMap.mapValues(_ + lhsMarkerPos + 1).map(identity)

    join.map(row => {
      val inLhs = row.getAs(lhsMarker) != null
      val inRhs = row.getAs(rhsMarker) != null
      val vals: Seq[Any] = outputSchema.fieldNames map (attr => {
        if (joinKeys.contains(attr)) {
          row.get(lhsColPos(attr))
        } else if (uniqueColsFromLhs.contains(attr)) {
          row.get(lhsColPos(attr))
        } else if (uniqueColsFromRhs.contains(attr)) {
          row.get(rhsColPos(attr))
        } else if (!inLhs) {
          row.get(rhsColPos(attr))
        } else if (!inRhs) {
          row.get(lhsColPos(attr))
        } else if (colsFromLhs.contains(attr)) {
          row.get(lhsColPos(attr))
        } else {
          row.get(rhsColPos(attr))
        }
      })
      Row.fromSeq(vals)
    })(RowEncoder(outputSchema))
  }

  private def getDefaultRhsSeq(numOfInputs: Int, lhsIdx: Int): List[Int] = {
    (0 until numOfInputs).filter(_ != lhsIdx).toList
  }

}
