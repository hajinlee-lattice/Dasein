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
    val intersectCols = lhs.columns.intersect(rhs.columns).diff(joinKeys)
    val uniqueColsFromLhs = lhs.columns.diff(joinKeys.union(intersectCols))
    val uniqueColsFromRhs = rhs.columns.diff(joinKeys.union(intersectCols))

    if (overwriteByNull && uniqueColsFromLhs.isEmpty && uniqueColsFromRhs.isEmpty) {
      // no need for per row operation

      if (colsFromLhs.isEmpty) {
        overwrite(lhs, rhs, joinKeys)
      } else {
        val cols1 = lhs.columns.intersect(joinKeys.union(colsFromLhs.toSeq)) map col
        val lhs1 = if (cols1.length != lhs.columns.length) lhs.select(cols1: _*) else lhs
        val rhs1 = if (cols1.length != rhs.columns.length) rhs.select(cols1: _*) else rhs
        val merged1 = overwrite(rhs1, lhs1, joinKeys)
        val cols2 = lhs.columns.diff(colsFromLhs.toSeq) map col
        if (cols2.isEmpty) {
          merged1
        } else {
          val lhs2 = lhs.select(cols2: _*)
          val rhs2 = rhs.select(cols2: _*)
          val merged2 = overwrite(lhs2, rhs2, joinKeys)
          merged2.join(merged1, joinKeys)
        }
      }

    } else {
      // need to compute row by row

      val outputSchema = getOutputSchema(lhs, rhs, joinKeys)
      val join = joinWithMarkders(lhs, rhs, joinKeys)
      val (lhsColPos, rhsColPos) = getColPosOnBothSides(join)
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
            if (InterfaceName.AccountId.name().equals(attr) //
              && DataCloudConstants.ENTITY_ANONYMOUS_ID.equals(firstVal)) {
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

  /**
    * when lhs and rhs have the same schema,
    * and completely use rhs for overlapping rows
    */
  def overwrite(lhs: DataFrame, rhs: DataFrame, joinKeys: Seq[String]): DataFrame = {
    val rhsIds = rhs.select(joinKeys map col: _*).withColumn(rhsMarker, lit(true))
    val lhsAppend = lhs.join(rhsIds, joinKeys, "left") //
      .filter(col(rhsMarker).isNull).select(rhs.columns map col: _*)
    rhs union lhsAppend
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
