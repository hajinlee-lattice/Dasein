package com.latticeengines.spark.exposed.job.common

import com.latticeengines.domain.exposed.spark.common.GenerateChangeTableConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.MergeUtils
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._

class GenerateChangeTableJob extends AbstractSparkJob[GenerateChangeTableConfig]{

  override def runJob(spark: SparkSession, lattice: LatticeContext[GenerateChangeTableConfig]): Unit = {
    val config : GenerateChangeTableConfig = lattice.config
    val fromTable : DataFrame = lattice.input.head
    val toTable: DataFrame = lattice.input(1)
    val exclusionCols : Set[String] = if (config.getExclusionColumns == null) Set() else config.getExclusionColumns
      .asScala.toSet
    val joinKey = config.getJoinKey
    val attrsForbidToSet : Set[String] = if (config.getAttrsForbidToSet == null) Set() else config.getAttrsForbidToSet
      .asScala.toSet

    val commonColNames = fromTable.columns.intersect(toTable.columns).diff(Seq(joinKey).union(exclusionCols.toSeq))
    val deletedColNames = fromTable.columns.diff(toTable.columns.union(exclusionCols.toSeq))
    val newColNames = toTable.columns.diff(fromTable.columns.union(exclusionCols.toSeq))

    val hasColumnChange = newColNames.length > 0 && deletedColNames.length > 0
    val schema = toTable.schema
    val joined = MergeUtils.joinWithMarkers(fromTable, toTable, Seq(joinKey), "outer")
    val (fromColPos, toColPos) = MergeUtils.getColPosOnBothSides(joined)
    val (fromMarker, toMarker) = MergeUtils.joinMarkers()
    val bFromColPos = spark.sparkContext.broadcast(fromColPos)
    val bToColPos = spark.sparkContext.broadcast(toColPos)
    val bAttrsForbidToSet = spark.sparkContext.broadcast(attrsForbidToSet + joinKey)
    val bColChange = spark.sparkContext.broadcast(hasColumnChange)

    val table: DataFrame = joined.flatMap((row : Row) => {
      val fColPos = bFromColPos.value
      val tColPos = bToColPos.value
      val forbidSet = bAttrsForbidToSet.value
      val columnChange = bColChange.value
      val isNew  = row.getAs(fromMarker) == null && row.getAs(toMarker) != null
      val isDelete = row.getAs(fromMarker) != null && row.getAs(toMarker) == null

      val rows : Option[Row] = {
        val values: Seq[Any] = if (isNew) {
          schema.fieldNames map (name =>
            if (tColPos.contains(name))
              row.get(tColPos(name))
            else
              null
            )
        } else if (isDelete) {
          schema.fieldNames map (name =>
            if (forbidSet.contains(name) && fColPos.contains(name))
              row.get(fColPos(name))
            else
              null
            )
        } else {
          // check changes in joined table
          // 1 column name change 2. column value change
          if (columnChange) {
            schema.fieldNames map (name =>
              if (tColPos.contains(name))
                row.get(tColPos(name))
              else
                null
              )
          } else {
          val nameOpt: Option[String] = commonColNames.find(name => row.getAs(fColPos(name)) != row.getAs(tColPos(name)))
          nameOpt match {
            case None =>
              null
            case Some(nameStr) =>
              schema.fieldNames map (name =>
                if (tColPos.contains(name))
                  row.get(tColPos(name))
                else
                  null
                )
          }
        }
      }
      if (values != null)
        Some(Row.fromSeq(values))
      else None
    }
    rows
    })(RowEncoder(schema))

    lattice.output = table :: Nil

    }

}
