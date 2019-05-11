package com.latticeengines.spark.util

import com.latticeengines.domain.exposed.datacloud.DataCloudConstants
import com.latticeengines.domain.exposed.metadata.InterfaceName
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}

private[spark] object MergeUtils {

  private val lhsMarker = "__merge_marker_lhs__"
  private val rhsMarker = "__merge_marker_rhs__"

  def merge2(lhs: DataFrame, rhs: DataFrame, joinKeys: Seq[String], colsFromLhs: Set[String], overwriteByNull: Boolean): DataFrame = {
    val outputSchema = getOutputSchema(lhs, rhs, joinKeys)
    val join = joinWithMarkders(lhs, rhs, joinKeys)
    val (lhsColPos, rhsColPos) = getColPosOnBothSides(join)

    val intersectCols = lhs.columns.intersect(rhs.columns).diff(joinKeys)
    val uniqueColsFromLhs = lhs.columns.diff(joinKeys.union(intersectCols))
    val uniqueColsFromRhs = rhs.columns.diff(joinKeys.union(intersectCols))

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
        } else {
          val (firstVal, secondVal) =
            if (colsFromLhs.contains(attr)) {
              (row.get(lhsColPos(attr)), row.get(rhsColPos(attr)))
            } else {
              (row.get(rhsColPos(attr)), row.get(lhsColPos(attr)))
            }
          if (InterfaceName.AccountId.name().equals(attr) && DataCloudConstants.ENTITY_ANONYMOUS_ID.equals(firstVal)) {
            if (secondVal == null) {
              firstVal
            } else {
              secondVal
            }
          } else {
            if (overwriteByNull || firstVal != null) {
              firstVal
            } else {
              secondVal
            }
          }
        }
      })
      Row.fromSeq(vals)
    })(RowEncoder(outputSchema))
  }

  def concat2(lhs: DataFrame, rhs: DataFrame): DataFrame = {
    if (lhs.columns.diff(rhs.columns).length > 0 || rhs.columns.diff(lhs.columns).length > 0) {
      val outputSchema = getOutputSchema(lhs, rhs, Seq())
      val expandedLhs = expand(lhs, outputSchema)
      val expandedRhs = expand(rhs, outputSchema)
      expandedLhs.union(expandedRhs)
    } else {
      lhs.union(rhs)
    }
  }

  private def getOutputSchema(lhs: DataFrame, rhs: DataFrame, joinKeys: Seq[String]): StructType = {
    val intersectCols = lhs.columns.intersect(rhs.columns).diff(joinKeys)
    val uniqueColsFromRhs = rhs.columns.diff(joinKeys.union(intersectCols))
    if (uniqueColsFromRhs.length > 0) {
      val uniqueRhs = rhs.select(uniqueColsFromRhs map col: _*)
      StructType(lhs.schema union uniqueRhs.schema)
    } else {
      lhs.schema
    }
  }

  private def joinWithMarkders(lhs: DataFrame, rhs: DataFrame, joinKeys: Seq[String]): DataFrame = {
    val lhsWithMarker = lhs.withColumn(lhsMarker, lit(true))
    val rhsWithMarker = rhs.withColumn(rhsMarker, lit(true))
    lhsWithMarker.join(rhsWithMarker, joinKeys, "outer")
  }

  private def getColPosOnBothSides(join: DataFrame): (Map[String, Int], Map[String, Int]) = {
    val lhsMarkerPos = join.columns.indexOf(lhsMarker)
    val (lhsCols, rhsCols) = join.columns.splitAt(lhsMarkerPos + 1)
    val lhsColPos = lhsCols.view.zipWithIndex.toMap
    val rhsColPos = rhsCols.view.zipWithIndex.toMap.mapValues(_ + lhsMarkerPos + 1).map(identity)
    (lhsColPos, rhsColPos)
  }

  private def expand(df: DataFrame, expandedSchema: StructType): DataFrame = {
    val colPos = df.columns.view.zipWithIndex.toMap
    df.map(row => {
      val vals: Seq[Any] = expandedSchema.fieldNames map (attr => {
        if (colPos.contains(attr)) {
          row.get(colPos(attr))
        } else {
          null
        }
      })
      Row.fromSeq(vals)
    })(RowEncoder(expandedSchema))
  }

}
