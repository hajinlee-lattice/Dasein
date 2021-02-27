package com.latticeengines.spark.exposed.job.graph

import java.util.Map

import com.latticeengines.domain.exposed.spark.graph.ConvertAccountsToGraphJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.collection.mutable

class ConvertAccountsToGraphJob extends AbstractSparkJob[ConvertAccountsToGraphJobConfig] {

  private val VERTEX_ID = "VertexID"
  private val VERTEX_TYPE = "Type"
  private val VERTEX_SYSTEM_ID = "SystemID"
  private val VERTEX_VALUE = "VertexValue"

  private val EDGE_SRC = "Src"
  private val EDGE_DEST = "Dest"
  private val EDGE_PROPERTY = "Property"

  private val docV = "docV"
  private val idV = "IdV"
  private val systemName = "SystemName"
  private val systemId = "SystemID"
  private val accountId = "AccountID"
  private val templateId = "TemplateID"
  private val matchIds = "MatchIDs"
  private val uniqueId = "UniqueID"

  override def runJob(spark: SparkSession, lattice: LatticeContext[ConvertAccountsToGraphJobConfig]): Unit = {
    val config: ConvertAccountsToGraphJobConfig = lattice.config
    val inputs: List[DataFrame] = lattice.input
    val inputDescriptors: List[Map[String, String]] = config.getInputDescriptors.asScala.toList

    var vertices: List[List[Any]] = List()
    var edges: List[List[Any]] = List()

    for ((input, idx) <- inputs.zipWithIndex) {
      var descriptor = inputDescriptors(idx)
      val matchCols = descriptor.get(matchIds).split(",")
      val descriptorUniqueId = descriptor.get(uniqueId)

      input.collect().foreach(row => {
        val rowAccountId = row.getAs[String](accountId)
        val rowSystemName = row.getAs[String](systemName)
        val docVId = descriptorUniqueId + "_" + rowAccountId
        vertices = vertices :+ List(docVId, docV, rowSystemName, rowAccountId)

        for (matchId <- matchCols) {
          val rowMatchId = row.getAs[String](matchId)
          val idVId = descriptorUniqueId + "_" + rowMatchId
          vertices = vertices :+ List(idVId, idV, matchId, rowMatchId)

          val property = new mutable.HashMap[String, String]()
          edges = edges :+ List(docVId, idVId, property.toString())
        }
      })
    }

    val verticesDf = createVerticesDf(spark, vertices)
    val edgesDf = createEdgesDf(spark, edges)

    lattice.output = List(verticesDf, edgesDf)
  }

  private def createVerticesDf(spark: SparkSession, vertices: List[List[Any]]): DataFrame = {
    val verticesSchema = StructType(List( //
        StructField(VERTEX_ID, StringType, nullable = false), //
        StructField(VERTEX_TYPE, StringType, nullable = false), //
        StructField(VERTEX_SYSTEM_ID, StringType, nullable = false), //
        StructField(VERTEX_VALUE, StringType, nullable = false) //
    ))
    val data: RDD[Row] = spark.sparkContext.parallelize(vertices.map(row => Row(row: _*)))
    spark.createDataFrame(data, verticesSchema)
  }

    private def createEdgesDf(spark: SparkSession, edges: List[List[Any]]): DataFrame = {
    val edgesSchema = StructType(List( //
        StructField(EDGE_SRC, StringType, nullable = false), //
        StructField(EDGE_DEST, StringType, nullable = false), //
        StructField(EDGE_PROPERTY, StringType, nullable = false) //
    ))
    val data: RDD[Row] = spark.sparkContext.parallelize(edges.map(row => Row(row: _*)))
    spark.createDataFrame(data, edgesSchema)
  }
}
