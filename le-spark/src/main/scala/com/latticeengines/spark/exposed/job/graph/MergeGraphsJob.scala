package com.latticeengines.spark.exposed.job.graph

import java.security.MessageDigest;
import java.util.Base64

import com.latticeengines.domain.exposed.spark.graph.MergeGraphsJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ConnectedComponents
import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.graphframes.GraphFrame
import scala.collection.Set
import scala.collection.mutable.HashMap

class MergeGraphsJob extends AbstractSparkJob[MergeGraphsJobConfig] {
  private val VERTEX_ID = "id"
  private val CONNECTED_COMPONENT_ID = "ConnectedComponentID"

  override def runJob(spark: SparkSession, lattice: LatticeContext[MergeGraphsJobConfig]): Unit = {
    val config: MergeGraphsJobConfig = lattice.config
    val inputs: List[DataFrame] = lattice.input

    // Step 1. Seperate vertexInputs and edgeInputs
    // and merge each of them into one dataframe
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

    // Step 2. Find duplicate vertices and update both vertices and edges
    var vertexRef = new HashMap[String, Long]()
    var convertEdge = new HashMap[Long, Long]()
    verticesDf.collect().map(row => {
      if (row.getAs[String](1) == "IdV") {
        val vertexId: Long = row.getAs[Long](0)
        val property: String = row.getAs[String](2) + "-" + row.getAs[String](3)
        if (vertexRef.contains(property)) {
          convertEdge += (vertexId -> vertexRef.getOrElse(property, 0L))
        } else {
          vertexRef += (property -> vertexId)
        }
      }
    })

    val updatedVerticesDf: DataFrame = updateVertices(convertEdge.keySet, verticesDf)
    val updatedEdgesDf: DataFrame = updateEdges(convertEdge, edgesDf)

    // Step 3. Create GraphFrame and run ConnectedComponents
    val graphFrame = GraphFrame(updatedVerticesDf, updatedEdgesDf)
    val components: DataFrame = graphFrame.connectedComponents.run()

    // Step 4. Encode CC ID and add it to VerticesDF 
    val encodeIdFunc: Long => String = ccId => {
      val addZeros = "%09d".format(ccId).toString()
      val withMd5 = MessageDigest.getInstance("MD5").digest(addZeros.getBytes)
      val result = Base64.getEncoder.encodeToString(withMd5)
      result
    }
    val componentsIdUDF = udf(encodeIdFunc)
    val componentsDf = components
        .select("id", "component")
        .withColumn("component", componentsIdUDF(col("component")))
        .withColumnRenamed("component", CONNECTED_COMPONENT_ID)

    val verticesWithCcDf: DataFrame = updatedVerticesDf
        .join(componentsDf, Seq(VERTEX_ID), "left")

    lattice.output = List(verticesWithCcDf, updatedEdgesDf)
  }

  private def updateEdges(convertEdge: HashMap[Long, Long], edgesDf: DataFrame): DataFrame = {
    val updateFunc: Long => Long = dst => {
      if (convertEdge.contains(dst)) convertEdge.getOrElse(dst, dst) else dst
    }
    val updateEdge = udf(updateFunc)
    edgesDf.withColumn("dst", updateEdge(col("dst")))
  }

  private def updateVertices(duplicateVertices: Set[Long], verticesDf: DataFrame): DataFrame = {
    val checkList = duplicateVertices.toList
    verticesDf.filter(!col("id").isin(checkList:_*))
  }
}
