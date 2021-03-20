package com.latticeengines.spark.exposed.job.graph

import com.latticeengines.domain.exposed.spark.graph.AssignEntityIdsJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{count, col}
import org.apache.spark.sql.expressions.Window
import org.graphframes.GraphFrame

import scala.collection.mutable.{HashMap, Set}

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
    val initialGraph: Graph[PregelVertexAttr, String] = graph.mapVertices((id, _) => {
      var messageList: List[VertexMessage] = List()
      var initMessage = false
      if (fromToMap.contains(id)) {
        initMessage = true
      }
      new PregelVertexAttr(toFromMap.getOrElse(id, -1L), messageList, initMessage, List())
    })

    val pregelGraph: Graph[PregelVertexAttr, String] = runPregel(initialGraph, maxComponentSize)

    var allConflictPaths: List[List[PathPair]] = List()
    pregelGraph.mapVertices((_, attr) => {
      if (!attr.foundPaths.isEmpty) {
        for (foundPath <- attr.foundPaths) {
          allConflictPaths = allConflictPaths :+ foundPath.path
        }
      }
    })

    // Step 4. Cut conflicts
    

    lattice.outputStr = allConflictPaths.mkString("\n")
    lattice.output = List(vertices, edges, conflictIdsDf)
  }

  private def runPregel(graph: Graph[PregelVertexAttr, String], maxIter: Int): Graph[PregelVertexAttr, String] = {
    val emptyMessageList: List[VertexMessage] = List()
    graph.pregel(emptyMessageList, maxIter, EdgeDirection.Either)(
      // Vertex Program
      (id, attr, incomingMessages) => {
        var messageList: List[VertexMessage] = List()
        var initMessage: Boolean = attr.initMessage
        if (initMessage) {
          val m = new VertexMessage(id, id, List(), Set())
          messageList = messageList :+ m
          initMessage = false
        }

        var newFoundPaths = attr.foundPaths
        incomingMessages.map(message => {
          // Add the latest pair to the path
          var path = message.path
          if (path.size % 2 == 1) {
            path = path :+ new PathPair(message.from, id)
          } else {
            path = path :+ new PathPair(id, message.from)
          }
          val newMessage = new VertexMessage(message.src, id, path, Set())

          // check if we are at destination
          if (attr.waitingFor == message.src) {
            // completed a path! Add it to the foundPaths
            newFoundPaths = newFoundPaths :+ newMessage

          } else if (message.path.iterator.exists(pair => pair.docVId == id || pair.idVId == id)) {
            // cycle: do not pass the message
          } else {
            // pass message to neighboring vertices
            messageList = messageList :+ newMessage
          }
        })
        new PregelVertexAttr(attr.waitingFor, messageList, initMessage, newFoundPaths)
      },

      // Send outgoing messages
      triplet => {
        // Set iterator for sending messages to both directions src <-> dst
        // Sent check is to prevent sending the same message in multiple iterations
        triplet.srcAttr.messageList.iterator.filter(m => !m.sent.contains(triplet.dstId)).map(m => {
          m.sent.add(triplet.dstId)
          (triplet.dstId, List(m))
        }).++(triplet.dstAttr.messageList.iterator.filter(m => !m.sent.contains(triplet.srcId)).map(m => {
          m.sent.add(triplet.srcId)
          (triplet.srcId, List(m))
        }))
      },
      // Merge messages: just concatenate the lists of incoming VertexMessages
      (a, b) => (a ++: b)
    )
  }
}

class PregelVertexAttr(var waitingFor: Long, var messageList: List[VertexMessage], var initMessage: Boolean,//
                             var foundPaths: List[VertexMessage]) extends Serializable {
  override def toString(): String = {
    "waitingFor: " + waitingFor + " foundPaths: " + foundPaths.toString()
  } 
}

class VertexMessage(var src: Long, var from: Long, var path: List[PathPair], var sent: Set[Long]) extends Serializable {
  override def toString(): String = {
    "src: " + src + ", path: " + path.toString()
  }
}

class PathPair(var docVId: Long, var idVId: Long) extends Serializable {
  override def toString(): String = {
    "(" + docVId + ", " + idVId + ")"
  }
}
