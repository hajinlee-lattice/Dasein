package com.latticeengines.spark.exposed.job.graph

import java.util.UUID

import com.latticeengines.domain.exposed.spark.graph.AssignEntityIdsJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, max, udf}
import org.apache.spark.sql.types._
import org.graphframes.GraphFrame

import scala.collection.mutable.{HashMap, Map, ListBuffer, PriorityQueue, Set}
import scala.collection.JavaConverters._
import scala.collection.Seq

class AssignEntityIdsJob extends AbstractSparkJob[AssignEntityIdsJobConfig] {
  private val componentId = "ConnectedComponentID"
  private val component = "component"
  private val entityId = "entityID"
  private val systemId = "systemID"
  private val vertexType = "type"
  private val vertexValue = "vertexValue"
  private val countCol = "count"
  private val idV = "IdV"
  private val docV = "docV"
  private val from = "from"
  private val fromId = "fromID"
  private val to = "to"
  private val toId = "toID"
  private val otherIdVId = "otherIdVID"

  private val edgesSchema = StructType(List( //
    StructField("src", LongType, nullable = false), //
    StructField("dst", LongType, nullable = false), //
    StructField("property", StringType, nullable = false)
  ))

  private var componentsWithConflicts: Set[String] = Set()

  override def runJob(spark: SparkSession, lattice: LatticeContext[AssignEntityIdsJobConfig]): Unit = {
    val config: AssignEntityIdsJobConfig = lattice.config
    // Key is "docV-idV" and the value is confidence score
    // The higher the more confident that the pair is accurate
    val matchConfidenceScore: Map[String, Integer] = config.getMatchConfidenceScore.asScala
    val vertices: DataFrame = lattice.input.head
    val edges: DataFrame = lattice.input(1)

    val maxComponentSize: Int = vertices
      .groupBy(componentId).count().agg(max(countCol).as(countCol))
      .orderBy(col(countCol).desc)
      .first.getAs[Long](countCol).intValue()

    val conflictIdsDf: DataFrame = generateConflictIdsDf(vertices)
    val fromToMap: Map[Long, Long] = collection.mutable.Map({
      conflictIdsDf.rdd.map {
        case Row(from: Long, to: Long) => (from, to)
      }.collectAsMap.toSeq: _*
    })
    val toFromMap: Map[Long, Long] = fromToMap.map(_.swap)

    val graph = generateFilteredGraph(vertices, edges, componentsWithConflicts)
    val pregelGraph: Graph[PregelVertexAttr, String] = initiateGraphAndRunPregel(graph, maxComponentSize, fromToMap, toFromMap)
    val edgesToRemove: HashMap[Long, Long] = calculateEdgesToRemove(pregelGraph, matchConfidenceScore)

    val updatedEdgesDf: DataFrame = updateEdgesDf(spark, edges, edgesToRemove)
    val entityIdsDf = generateEntityIdsDf(updatedEdgesDf, vertices)
    val verticesWithEntityIdsDf: DataFrame = vertices.join(entityIdsDf, Seq("id"), "left")

    val inconsistencyReportDf: DataFrame = generateInconsistencyReportDf(spark, verticesWithEntityIdsDf, fromToMap, toFromMap)

    lattice.output = List(verticesWithEntityIdsDf, updatedEdgesDf, inconsistencyReportDf)
  }

  private def updateEdgesDf(spark: SparkSession, edges: DataFrame, edgesToRemove: HashMap[Long, Long]): DataFrame = {
    val updatedEdgeRdd: RDD[Row] = edges
      .rdd.filter {
        case Row(src, dst, prop) => {
          !(edgesToRemove.contains(src.asInstanceOf[Long]) && edgesToRemove.getOrElse(src.asInstanceOf[Long], -1L).equals(dst.asInstanceOf[Long]))
        }
      }
    spark.createDataFrame(updatedEdgeRdd, edgesSchema)
  }

  private def calculateEdgesToRemove(pregelGraph: Graph[PregelVertexAttr, String], matchConfidenceScore: Map[String, Integer]): HashMap[Long, Long] = {
    val allConflictPaths: RDD[List[PathPair]] = pregelGraph.vertices.flatMap(
      { case (_, attr) => attr.foundPaths.map((p) => p.path) }
    )

    var allPaths: List[List[PathPair]] = allConflictPaths.collect.toList
    var allPairsByTypes: HashMap[String, Set[PathPair]] = HashMap[String, Set[PathPair]]()
    allPaths.flatten.foreach(pathPair => {
      if (allPairsByTypes.contains(pathPair.pairType)) {
        allPairsByTypes.getOrElse(pathPair.pairType, Set()).add(pathPair)
      } else {
        allPairsByTypes += (pathPair.pairType -> Set(pathPair))
      }
    })

    var removedTypes: Set[String] = Set()
    val matchConfidenceRanking: PriorityQueue[TypeConfidence] = constructConfidenceQueue(matchConfidenceScore, allPairsByTypes)
    while (!allPaths.isEmpty && !matchConfidenceRanking.isEmpty) {
      val lowestType = matchConfidenceRanking.dequeue.name
      removedTypes.add(lowestType)
      allPaths = allPaths.filter(pathPair => !pathPair.exists(p => p.pairType == lowestType))
    }

    var edgesToRemove: HashMap[Long, Long] = HashMap[Long, Long]()
    for (removedType <- removedTypes) {
      val pairSet = allPairsByTypes.getOrElse(removedType, Set())
      pairSet.map(pair => edgesToRemove += (pair.docVId -> pair.idVId))
    }
    edgesToRemove
  }

  private def generateFilteredGraph(vertices: DataFrame, edges: DataFrame, componentsWithConflicts: Set[String]) = {
    val conflictDocVs = vertices
      .filter(col(vertexType) === docV && col(componentId).isin(componentsWithConflicts.toList:_*))
      .select("id").rdd.map(r => r.getAs[Long](0)).collect

    val erdd: RDD[Edge[String]] = edges
      .filter(col("src").isin(conflictDocVs:_*))
      .rdd.map(row => Edge(row.getAs[VertexId](0), row.getAs[VertexId](1), row.getAs[String](2)))

    val vrdd: RDD[(VertexId, (String /* type */, String /* systemID */, String /* VertexValue */, String /* ComponentID */))] = vertices
      .rdd.map(row => (
        row.getAs[VertexId](0), (
          row.getAs[String](1),
          row.getAs[String](2),
          row.getAs[String](3),
          row.getAs[String](4)
        ))
      )

    Graph(vrdd, erdd)
  }

  private def generateEntityIdsDf(edgesDf: DataFrame, vertices: DataFrame): DataFrame = {
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

    entityDf
      .select("id", component)
      .withColumn(component, entityIdUDF(col(component)))
      .withColumnRenamed(component, entityId)
  }

  private def generateConflictIdsDf(vertices: DataFrame): DataFrame = {
    // conflictsDf should have columns - id, systemId, componentId
    // with only rows that are conflicting IdVs
    val w = Window.partitionBy(componentId, systemId)
    val conflictsDf: DataFrame = vertices
      .filter(col(vertexType) === idV)
      .withColumn(countCol, count(systemId).over(w))
      .filter(col(countCol) > 1)
      .drop(countCol, vertexType, vertexValue)

    val conflictComponents = conflictsDf.select(componentId).rdd.map(r => r.getAs[String](0)).collect
    componentsWithConflicts = Set(conflictComponents:_*)

    // conflictIdsDf should have columns - fromId, toId
    // with each row representing a unique conflict pair
    conflictsDf.alias(from).withColumnRenamed("id", fromId)
      .join(conflictsDf.alias(to).withColumnRenamed("id", toId), Seq(componentId, systemId), "outer")
      .filter(col(fromId) < col(toId))
      .drop(componentId, systemId)
  }

  private def generateInconsistencyReportDf(spark: SparkSession, vertexDf: DataFrame, fromToMap: Map[Long, Long], //
      toFromMap: Map[Long, Long]): DataFrame = {

    var ids: List[Long] = fromToMap.keys.toList ++ toFromMap.keys.toList
    val conflictVertices = vertexDf.filter(col("id").isin(ids:_*)).drop(vertexType)
    val otherVertices = conflictVertices.withColumnRenamed("id", otherIdVId).drop(entityId, vertexValue)

    // inconsistencyReport columns: systemId, componentId, id, vertexValue, entityId, otherIdVId
    conflictVertices
      .join(otherVertices.alias("other"), Seq(systemId, componentId), "left")
      .filter(col("id").notEqual(col(otherIdVId)))
  }

  private def initiateGraphAndRunPregel(graph: Graph[(String, String, String, String), String], maxComponentSize: Int, //
      fromToMap: Map[Long, Long], toFromMap: Map[Long, Long]): Graph[PregelVertexAttr, String] = {

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

    runPregel(initialGraph, maxComponentSize)
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
            println(s"[runPregel]: Cycle detected at vertex $id")
          } else {
            // pass newMessage to neighboring vertices
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
    // Returns a priority queue in a way that least confident type gets dequeued first
    // it first compares matchConfidenceScore, then the number of pairs found in each type (the fewer the less confident)
    var typeList: ListBuffer[TypeConfidence] = ListBuffer()
    for (key <- allPairsByTypes.keySet) {
      val score: Int = matchConfidenceScore.getOrElse(key, 0).asInstanceOf[Int]
      val confidence = new TypeConfidence(key, score, allPairsByTypes.getOrElse(key, Set()).size)
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
