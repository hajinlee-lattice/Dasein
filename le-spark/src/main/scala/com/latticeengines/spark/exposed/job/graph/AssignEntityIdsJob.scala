package com.latticeengines.spark.exposed.job.graph

import java.util.Map
import java.util.UUID

import com.latticeengines.domain.exposed.spark.graph.AssignEntityIdsJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, udf}
import org.apache.spark.sql.types._
import org.graphframes.GraphFrame

import scala.collection.mutable.{HashMap, ListBuffer, PriorityQueue, Set}

class AssignEntityIdsJob extends AbstractSparkJob[AssignEntityIdsJobConfig] {
  private val componentId = "ConnectedComponentID"

  private val edgesSchema = StructType(List( //
    StructField("src", LongType, nullable = false), //
    StructField("dst", LongType, nullable = false), //
    StructField("property", StringType, nullable = false)
  ))

  private val inconsistencyReportSchema = StructType(List( //
    StructField("ConnectedComponentID", StringType, nullable = false), //
    StructField("entityID", StringType, nullable = false), //
    StructField("systemID", StringType, nullable = false), //
    StructField("matchID", StringType, nullable = false), //
    StructField("IdVID", LongType, nullable = false), //
    StructField("otherIdVID", LongType, nullable = false)
  ))

  override def runJob(spark: SparkSession, lattice: LatticeContext[AssignEntityIdsJobConfig]): Unit = {
    val config: AssignEntityIdsJobConfig = lattice.config
    val matchConfidenceScore: Map[String, Integer] = config.getMatchConfidenceScore
    val vertices: DataFrame = lattice.input.head
    val edges: DataFrame = lattice.input(1)

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
    val initialGraph: Graph[PregelVertexAttr, String] = graph.mapVertices {
      case (id, (_, system, _, _)) => {
        var systemId: String = system
        var messageList: List[VertexMessage] = List()
        var initMessage = false
        if (fromToMap.contains(id)) {
          initMessage = true
        }
        new PregelVertexAttr(toFromMap.getOrElse(id, -1L), messageList, initMessage, List(), systemId)
      }
    }

    val pregelGraph: Graph[PregelVertexAttr, String] = runPregel(initialGraph, maxComponentSize)

    var allConflictPaths: List[List[PathPair]] = List()
    pregelGraph.vertices.collect().map({ case (id, attr) => 
      if (!attr.foundPaths.isEmpty) {
        for (foundPath <- attr.foundPaths) {
          allConflictPaths = allConflictPaths :+ foundPath.path
        }
      }
    })

    // Step 4. Construct priority queue and remove edges from the least confident types
    val allPairsByTypes: HashMap[String, Set[PathPair]] = HashMap[String, Set[PathPair]]()
    allConflictPaths.flatten.map(pathPair => {
      if (allPairsByTypes.contains(pathPair.pairType)) {
        allPairsByTypes.getOrElse(pathPair.pairType, Set()).add(pathPair)
      } else {
        allPairsByTypes += (pathPair.pairType -> Set(pathPair))
      }
    })

    var removedTypes: Set[String] = Set()
    val matchConfidenceRanking: PriorityQueue[TypeConfidence] = constructConfidenceQueue(matchConfidenceScore, allPairsByTypes)
    while (!allConflictPaths.isEmpty && !matchConfidenceRanking.isEmpty) {
      val lowestType = matchConfidenceRanking.dequeue.name
      removedTypes.add(lowestType)
      allConflictPaths = allConflictPaths.filter(path => !path.exists(p => p.pairType == lowestType))
    }

    var edgesToRemove: HashMap[Long, Long] = HashMap[Long, Long]()
    for (removedType <- removedTypes) {
      val pairSet = allPairsByTypes.getOrElse(removedType, Set())
      pairSet.map(pair => edgesToRemove += (pair.docVId -> pair.idVId))
    }

    val updatedEdgeRdd: RDD[Edge[String]] = erdd.filter {
      case Edge(src, dst, prop) => !(edgesToRemove.contains(src) && edgesToRemove.getOrElse(src, -1L).equals(dst))
    }

    // Step 5. Run ConnectedComponents again to assign Entity ID
    val edgeRows: RDD[Row] = updatedEdgeRdd.map {
      case Edge(src, dst, prop) => Row(src, dst, prop)
    }
    val edgesDf: DataFrame = spark.createDataFrame(edgeRows, edgesSchema)
    val graphFrame = GraphFrame(vertices, edgesDf)
    val entityDf: DataFrame = graphFrame.connectedComponents.run()

    var seenCcId: HashMap[Long, String] = HashMap[Long, String]()
    val entityIdFunc: Long => String = ccId => {
      if (seenCcId.contains(ccId)) {
        seenCcId.getOrElse(ccId, UUID.randomUUID().toString)
      } else {
        var newId: String = UUID.randomUUID().toString
        seenCcId += (ccId -> newId)
        newId
      }
    }
    val entityIdUDF = udf(entityIdFunc)
    val entityIdsDf = entityDf
        .select("id", "component")
        .withColumn("component", entityIdUDF(col("component")))
        .withColumnRenamed("component", "entityID")

    val verticesWithEntityIdsDf: DataFrame = vertices
        .join(entityIdsDf, Seq("id"), "left")

    // Step 6. Generate Inconsistency Report
    var inconsistencyRows: List[List[Any]] = List()
    verticesWithEntityIdsDf.collect.map(row => {
      val vertexId = row.getAs[Long](0)
      if (fromToMap.contains(vertexId) || toFromMap.contains(vertexId)) {
        val systemId = row.getAs[String](2)
        val matchId = row.getAs[String](3)
        val componentId = row.getAs[String](4)
        val entityId = row.getAs[String](5)
        val otherIdVId = ( if (fromToMap.contains(vertexId)) fromToMap.getOrElse(vertexId, -1L) else toFromMap.getOrElse(vertexId, -1L) )
        inconsistencyRows = inconsistencyRows :+ List(componentId, entityId, systemId, matchId, vertexId, otherIdVId)
      }
    })

    val inconsistencyData: RDD[Row] = spark.sparkContext.parallelize(inconsistencyRows.map(row => Row(row: _*)))
    val inconsistencyReportDf: DataFrame = spark.createDataFrame(inconsistencyData, inconsistencyReportSchema)

    lattice.output = List(verticesWithEntityIdsDf, edgesDf, inconsistencyReportDf)
  }

  private def runPregel(graph: Graph[PregelVertexAttr, String], maxIter: Int): Graph[PregelVertexAttr, String] = {
    val emptyMessageList: List[VertexMessage] = List()
    graph.pregel(emptyMessageList, maxIter, EdgeDirection.Either)(
      // Vertex Program
      (id, attr, incomingMessages) => {
        var systemId: String = attr.systemId
        var messageList: List[VertexMessage] = List()
        var initMessage: Boolean = attr.initMessage
        if (initMessage) {
          val m = new VertexMessage(id, id, systemId, List(), Set())
          messageList = messageList :+ m
          initMessage = false
        }

        var newFoundPaths = attr.foundPaths
        incomingMessages.map(message => {
          // Add the latest pair to the path
          var path = message.path
          var pairType: String = ""
          if (path.size % 2 == 1) {
            pairType = message.fromSystem + "-" + systemId
            path = path :+ new PathPair(message.from, id, pairType)
          } else {
            pairType = systemId + "-" + message.fromSystem
            path = path :+ new PathPair(id, message.from, pairType)
          }
          val newMessage = new VertexMessage(message.src, id, systemId, path, Set())

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
        new PregelVertexAttr(attr.waitingFor, messageList, initMessage, newFoundPaths, systemId)
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

  private def constructConfidenceQueue(matchConfidenceScore: Map[String, Integer], //
                                       allPairsByTypes: HashMap[String, Set[PathPair]]): PriorityQueue[TypeConfidence] = {
    var typeList: ListBuffer[TypeConfidence] = ListBuffer()
    for (key <- allPairsByTypes.keySet) {
      val confidence = new TypeConfidence(key, matchConfidenceScore.getOrDefault(key, 0), allPairsByTypes.getOrElse(key, Set()).size)
      typeList += confidence
    }

    var pq = new PriorityQueue[TypeConfidence]()(Ordering.by(t => (-t.score, -t.numPairs)))
    typeList.map(t => pq.enqueue(t))
    pq
  }
}

class PregelVertexAttr(var waitingFor: Long, var messageList: List[VertexMessage], var initMessage: Boolean,//
                       var foundPaths: List[VertexMessage], var systemId: String) extends Serializable {
  override def toString(): String = {
    "waitingFor: " + waitingFor + " foundPaths: " + foundPaths.toString()
  } 
}

class VertexMessage(var src: Long, var from: Long, var fromSystem: String, var path: List[PathPair], var sent: Set[Long]) extends Serializable {
  override def toString(): String = {
    "src: " + src + ", path: " + path.toString()
  }
}

case class PathPair(var docVId: Long, var idVId: Long, var pairType: String) extends Serializable {
  override def toString(): String = {
    "(" + pairType + ", " + docVId + ", " + idVId + ")"
  }

  override def equals(that: Any): Boolean =
    that match {
      case that: PathPair => {
        this.docVId.equals(that.docVId) &&
        this.idVId.equals(that.idVId) &&
        this.pairType.equals(that.pairType)
      }
      case _ => false
    }
}

case class TypeConfidence(var name: String, var score: Int, var numPairs: Int)
