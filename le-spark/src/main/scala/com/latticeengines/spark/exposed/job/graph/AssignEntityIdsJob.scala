package com.latticeengines.spark.exposed.job.graph

import com.latticeengines.domain.exposed.spark.graph.AssignEntityIdsJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{count, col}
import org.apache.spark.sql.expressions.Window
import org.graphframes.GraphFrame

import scala.collection.mutable.HashMap

class AssignEntityIdsJob extends AbstractSparkJob[AssignEntityIdsJobConfig] {
  private val componentId = "ConnectedComponentID"

  override def runJob(spark: SparkSession, lattice: LatticeContext[AssignEntityIdsJobConfig]): Unit = {
    val config: AssignEntityIdsJobConfig = lattice.config
    val vertices: DataFrame = lattice.input.head
    val edges: DataFrame = lattice.input(1)
    // val confidenceScores: HashMap[String, Int] = config.getConfidenceScores

    // Step 1. Get the list of conflicting IDs in pairs, from and to
    var w = Window.partitionBy(componentId, "systemID")
    var conflictsDf: DataFrame = vertices
      .withColumn("idCount", count("systemID").over(w))
      .filter(col("idCount") > 1 && col("type") === "IdV")
      .drop("idCount", "type", "vertexValue")

    val conflictIdsDf: DataFrame = conflictsDf.alias("from").withColumnRenamed("id", "fromId")
      .crossJoin(conflictsDf.alias("to").withColumnRenamed("id", "toId"))
      .where(col("from.ConnectedComponentID") === col("to.ConnectedComponentID") && col("from.systemID") === col("to.systemID"))
      .select("fromId", "toId").orderBy("fromId")
      .filter(col("fromId") < col("toId"))
    
    val maxComponentSize: Int = vertices
      .withColumn("count", count(componentId).over(Window.partitionBy(componentId)))
      .orderBy(col("count").desc).first.getAs[Long](5).intValue()

    var fromToMap: HashMap[Long, Long] = HashMap[Long, Long]()
    var toFromMap: HashMap[Long, Long] = HashMap[Long, Long]()
    conflictIdsDf.collect().map(row => {
      val from: Long = row.getAs[Long](0)
      val to: Long = row.getAs[Long](1)
      fromToMap += (from -> to)
      toFromMap += (to -> from)
    })

    // Step 2. Generate Graph
    val vrdd: RDD[(VertexId, (String /* type */, String /* systemID */, String /* VertexValue */, String /* ComponentID */))] = vertices
      .rdd.map(row => (
        row.getAs[VertexId](0), (
          row.getAs[String](1),
          row.getAs[String](2),
          row.getAs[String](3),
          row.getAs[String](4)
        ))
      )
    val erdd: RDD[Edge[String]] = edges
      .rdd.map(row =>
        Edge(row.getAs[VertexId](0), row.getAs[VertexId](1), row.getAs[String](2))
      )
    val graph = Graph(vrdd, erdd)

    // Step 3. Get all paths between conflicting pairs using pregel
    val initialGraph: Graph[PregelVertexAttributes, String] = graph.mapVertices((id, _) => {
      var messageBuffer: List[PathFinder] = List()
      var initMessage: List[PathFinder] = List()
      if (fromToMap.contains(id)) {
        val pf = new PathFinder(id, id, List())
        initMessage = initMessage :+ pf
      }
      new PregelVertexAttributes(toFromMap.getOrElse(id, -1L), messageBuffer, initMessage, List())
    })

    val pregelGraph = runPregel(initialGraph, 1)

    // Step 4. Cut conflicts
    lattice.outputStr = pregelGraph.vertices.collect().mkString("\n")
    lattice.output = List(vertices, edges, conflictIdsDf)
  }

  private def runPregel(graph: Graph[PregelVertexAttributes, String], maxIter: Int): Graph[PregelVertexAttributes, String] = {
    val emptyPfList: List[PathFinder] = List()
    graph.pregel(emptyPfList, maxIter, EdgeDirection.Either)(
      // Vertex Program
      (id, prop, listOfPathFinders) => {
        var messageBuffer: List[PathFinder] = prop.initMessage // contains initial messages at iteration 0
        var initMessage: List[PathFinder] = List() // empty the init message

        var newFoundPaths = prop.foundPaths
        listOfPathFinders.map(pf => {
          // update path based on path size to always have docV ID first
          var path = pf.path
          if (path.size % 2 == 1) {
            path = path :+ new PathPair(pf.from, id)
          } else {
            path = path :+ new PathPair(id, pf.from)
          }
          val newPf = new PathFinder(pf.src, id, path)

          // check if we are a "to" destination
          if (prop.waitingFor == pf.src) {
            // completed a path! Add it to the list of found paths
            newFoundPaths = newFoundPaths :+ newPf
            // send an emtpy message to the previous one to trigger vprogram
            // val emptyPf = new PathFinder(id, id, List())
            // messageBuffer = messageBuffer :+ emptyPf
          } else if (pf.path.iterator.exists(pair => pair.docVId == id || pair.idVId == id)) {
            // cycle: do not pass the message
          } else {
            // pass message to neighboring vertices
            messageBuffer = messageBuffer :+ newPf // this will be sent this step
          }
        })
        new PregelVertexAttributes(prop.waitingFor, messageBuffer, initMessage, newFoundPaths)
      },

      // Send Outgoing Messages
      triplet => {
        // Set iterator for sending messages to both directions src <-> dst
        triplet.srcAttr.messageBuffer.iterator.map(pf => (triplet.dstId, List(pf)))
          .++(triplet.dstAttr.messageBuffer.iterator.map(pf => (triplet.srcId, List(pf))))
      },
      // Merge messages: just concatenate the lists of incoming PathFinders
      (a, b) => (a ++: b)
    )
  }
}

class PregelVertexAttributes(var waitingFor: Long, var messageBuffer: List[PathFinder], var initMessage: List[PathFinder],//
                             var foundPaths: List[PathFinder]) extends Serializable {
  override def toString(): String = {
    "waitingFor: " + waitingFor + " foundPaths: " + foundPaths.toString() + " messageBuffer: " + messageBuffer.toString()
  } 
}

class PathFinder(val src: Long, var from: Long, var path: List[PathPair]) extends Serializable {
  override def toString(): String = {
    "src: " + src + " path: " + path.toString()
  }
}

class PathPair(var docVId: Long, var idVId: Long) extends Serializable {
  override def toString(): String = {
    "(" + docVId + ", " + idVId + ")"
  }
}
