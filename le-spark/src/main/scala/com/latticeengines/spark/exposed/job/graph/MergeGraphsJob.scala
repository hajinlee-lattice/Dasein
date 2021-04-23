package com.latticeengines.spark.exposed.job.graph

import java.security.MessageDigest;
import java.util.Base64

import com.latticeengines.domain.exposed.spark.graph.MergeGraphsJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ConnectedComponents
import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, min, udf}
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.graphframes.GraphFrame
import scala.collection.Set
import scala.collection.mutable.HashMap

class MergeGraphsJob extends AbstractSparkJob[MergeGraphsJobConfig] {
  private val VERTEX_ID = "id"
  private val CONNECTED_COMPONENT_ID = "ConnectedComponentID"
  private val SYSTEM_ID = "systemID"
  private val VERTEX_VALUE = "vertexValue"

  private val fromId = "fromID"
  private val toId = "toID"
  private val countCol = "count"
  private val component = "component"

  private val src = "src";
  private val dst = "dst";
  private val prop = "property";

  private val edgesSchema = StructType(List( //
    StructField(src, LongType, nullable = false), //
    StructField(dst, LongType, nullable = false), //
    StructField(prop, StringType, nullable = false)
  ))

  override def runJob(spark: SparkSession, lattice: LatticeContext[MergeGraphsJobConfig]): Unit = {
    val config: MergeGraphsJobConfig = lattice.config
    val inputs: List[DataFrame] = lattice.input

    val combinedDfs = combineInputs(inputs)
    val verticesDf = combinedDfs.head
    val edgesDf = combinedDfs.last

    val duplicateVertices: DataFrame = generateDuplicateVertices(verticesDf)
    val updatedVerticesDf: DataFrame = updateVertices(duplicateVertices, verticesDf)
    val updatedEdgesDf: DataFrame = updateEdges(spark, duplicateVertices, edgesDf)

    val graphFrame = GraphFrame(updatedVerticesDf, updatedEdgesDf)
    val components: DataFrame = graphFrame.connectedComponents.run()

    val verticesWithCcDf = generateVerticesWithComponents(components, updatedVerticesDf)

    lattice.output = List(verticesWithCcDf, updatedEdgesDf)
  }

  private def generateDuplicateVertices(vertices: DataFrame): DataFrame = {
    val w = Window.partitionBy(SYSTEM_ID, VERTEX_VALUE)
    vertices
      .withColumnRenamed("id", fromId)
      .withColumn(countCol, count(VERTEX_VALUE).over(w))
      .withColumn(toId, min(col(fromId)).over(w))
      .filter(col(countCol) > 1 && (col(fromId) !== col(toId)))
      .select(fromId, toId)
  }

  private def combineInputs(inputs: List[DataFrame]): List[DataFrame] = {
    var vertexInputs: List[DataFrame] = List()
    var edgeInputs: List[DataFrame] = List()
    for (input <- inputs) {
      if (input.columns.contains(VERTEX_ID)) {
        vertexInputs = vertexInputs :+ input
      } else {
        edgeInputs = edgeInputs :+ input
      }
    }

    val verticesDf: DataFrame = vertexInputs
      .reduce((a,b) => a.union(b))
    val edgesDf: DataFrame = edgeInputs
      .reduce((a,b) => a.union(b))

    List(verticesDf, edgesDf)
  }

  private def generateVerticesWithComponents(components: DataFrame, vertices: DataFrame): DataFrame = {
    val encodeIdFunc: Long => String = ccId => {
      val addZeros = "%09d".format(ccId).toString()
      val withMd5 = MessageDigest.getInstance("MD5").digest(addZeros.getBytes)
      val result = Base64.getEncoder.encodeToString(withMd5)
      result
    }
    val componentsIdUDF = udf(encodeIdFunc)
    val componentsDf = components
      .select("id", component)
      .withColumn(component, componentsIdUDF(col(component)))
      .withColumnRenamed(component, CONNECTED_COMPONENT_ID)

    vertices.join(componentsDf, Seq(VERTEX_ID), joinType="left")
  }

  private def updateEdges(spark: SparkSession, duplicateVertices: DataFrame, edgesDf: DataFrame): DataFrame = {
    val edgeRows: RDD[Row] = edgesDf
      .join(duplicateVertices.withColumnRenamed(fromId, dst), Seq(dst), joinType="left")
      .rdd.map(row => {
        val dst = row.getLong(0)
        val src = row.getLong(1)
        val prop = row.getString(2)
        val convertId = row(3) // long or null
        if (convertId != null) {
          Row(src, convertId, prop)
        } else {
          Row(src, dst, prop)
        }
      })

    spark.createDataFrame(edgeRows, edgesSchema)
  }

  private def updateVertices(duplicateVertices: DataFrame, verticesDf: DataFrame): DataFrame = {
    val dropVertices = duplicateVertices.withColumnRenamed(fromId, "id").drop(toId)

    verticesDf
      .join(dropVertices, Seq("id"), joinType="leftanti")
  }
}
