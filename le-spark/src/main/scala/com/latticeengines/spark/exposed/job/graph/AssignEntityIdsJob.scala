package com.latticeengines.spark.exposed.job.graph

import com.latticeengines.domain.exposed.spark.graph.AssignEntityIdsJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, max, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.graphframes.GraphFrame

import scala.collection.JavaConverters._
import scala.collection.mutable

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
  private val src = "src"
  private val dst = "dst"

  private val edgesSchema = StructType(List( //
    StructField(src, LongType, nullable = false), //
    StructField(dst, LongType, nullable = false), //
    StructField("property", StringType, nullable = false)
  ))

  private val edgePairSchema = StructType(List( //
    StructField(src, LongType, nullable = false), //
    StructField(dst, LongType, nullable = false)
  ))

  private type Path = List[PathPair]

  override def runJob(spark: SparkSession, lattice: LatticeContext[AssignEntityIdsJobConfig]): Unit = {
    val config: AssignEntityIdsJobConfig = lattice.config
    val edgeRank: Seq[String] =  config.getEdgeRank.asScala
    val vertices: DataFrame = lattice.input.head
    val edges: DataFrame = lattice.input(1)

    val maxComponentSize: Int = vertices
      .groupBy(componentId).count().agg(max(countCol).as(countCol))
      .orderBy(col(countCol).desc)
      .first.getAs[Long](countCol).intValue()

    val conflictIdsDf: DataFrame = generateConflictIdsDf(vertices)
    var componentsWithConflicts: Set[String] = getComponentsWithConflicts(conflictIdsDf)

    val graph = generateFilteredGraph(vertices, edges, componentsWithConflicts, conflictIdsDf)
    val pregelGraph: Graph[PregelVertexAttr, String] = initiateGraphAndRunPregel(graph, maxComponentSize)
    val edgesToRemove: RDD[Row] = calculateEdgesToRemove(spark, pregelGraph, edgeRank)
    val updatedEdgesDf: DataFrame = removeConflictingEdges(spark, edges, edgesToRemove)

    val entityIdsDf = generateEntityIdsDf(updatedEdgesDf, vertices)
    val verticesWithEntityIdsDf: DataFrame = vertices.join(entityIdsDf, Seq("id"), joinType="left")

    val inconsistencyReportDf: DataFrame = generateInconsistencyReportDf(verticesWithEntityIdsDf, conflictIdsDf)

    lattice.output = List(verticesWithEntityIdsDf, updatedEdgesDf, inconsistencyReportDf)
  }

  private def removeConflictingEdges(spark: SparkSession, edges: DataFrame, edgesToRemove: RDD[Row]): DataFrame = {
    val edgesToRemoveDf = spark.createDataFrame(edgesToRemove, edgePairSchema)
    edges.join(edgesToRemoveDf, Seq(src, dst), "leftanti")
  }

  private def calculateEdgesToRemove(spark: SparkSession, pregelGraph: Graph[PregelVertexAttr, String],
                                     edgeRank: Seq[String]): RDD[Row] = {
    val bcEdgeRank = spark.sparkContext.broadcast(edgeRank)

    val pairsToRemove: RDD[Row] = pregelGraph.vertices
      .flatMap({
        case (_, attr) => attr.foundPaths.map(p => (attr.componentId, p.path))
      }) // convert to (componentId, Path)
      .aggregateByKey(List[Path]())(
        (paths, path) => path :: paths,
        (paths1, paths2) => paths1 ::: paths2
      ) // for each component, aggregate all paths
      .flatMapValues(paths => { // assume all paths for a component fit in one executor's memory
        // sort distinct pairs desc by confidence score
        val edgeRank = bcEdgeRank.value
        val sortedPairs: Seq[PathPair] = paths.flatten.distinct
          .sortWith((pair1, pair2) => {
            val confidence1 = edgeRank.indexOf(pair1.pairType)
            val confidence2 = edgeRank.indexOf(pair2.pairType)
            if (confidence1 == confidence2) {
              pair1.pairHash < pair2.pairHash // sorting by composite key may handle arbitrary import order
            } else {
              confidence1 > confidence2
            }
          })

        // remove pairs from all paths one by one, until no more conflicting paths
        val removedPairs: mutable.ListBuffer[PathPair] = mutable.ListBuffer()
        def breakTies(): Option[List[Path]] = {
          sortedPairs.foldLeft(Some(paths))((conflictingPaths, leastConfidentPair) => {
            if (conflictingPaths.get.isEmpty) { // terminate fold if there are not conflicting paths
              return None
            }
            if (conflictingPaths.get.exists(_.contains(leastConfidentPair))) {
              removedPairs.append(leastConfidentPair)
              Some(conflictingPaths.get.filterNot(_.contains(leastConfidentPair)))
            } else {
              conflictingPaths
            }
          })
        }
        breakTies()
        removedPairs.toList
      }) // convert to edges to be removed
      .values.map(pair => Row(pair.docVId, pair.idVId))

    pairsToRemove
  }

  private def generateFilteredGraph(vertices: DataFrame, edges: DataFrame, componentsWithConflicts: Set[String], //
      conflictIdsDf: DataFrame):Graph[(String, String, String, String, Long), String] = {

    val conflictDocVs = vertices
      .filter(col(vertexType) === docV && col(componentId).isin(componentsWithConflicts.toList:_*))
      .select("id").rdd.map(r => r.getAs[Long](0)).collect

    val erdd: RDD[Edge[String]] = edges
      .filter(col(src).isin(conflictDocVs:_*))
      .rdd.map(row => Edge(row.getAs[VertexId](0), row.getAs[VertexId](1), row.getAs[String](2)))

    val combinedVertices: DataFrame = vertices
      .join(conflictIdsDf.drop(componentId).withColumnRenamed(fromId, "id"), Seq("id"), "left")

    val vrdd: RDD[(VertexId,
      (
        String /* type */, //
        String /* systemID */, //
        String /* VertexValue */, //
        String /* ComponentID */,
        Long /* toID */ //
      ))] = combinedVertices
      .rdd.map(row => (
        row.getAs[VertexId](0), (
          row.getAs[String](1),
          row.getAs[String](2),
          row.getAs[String](3),
          row.getAs[String](4),
          row.getAs[Long](5)
        ))
      )

    Graph(vrdd, erdd)
  }

  private def generateEntityIdsDf(edgesDf: DataFrame, vertices: DataFrame): DataFrame = {
    val graphFrame = GraphFrame(vertices, edgesDf)
    val entityDf: DataFrame = graphFrame.connectedComponents.run()

    val entityIdUDF = udf(() => java.util.UUID.randomUUID.toString)
    val entityIds: DataFrame = entityDf.select(component).distinct.withColumn(entityId, entityIdUDF())

    entityDf.join(entityIds, Seq(component), joinType = "inner").select("id", entityId)
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

    // conflictIdsDf should have columns - fromId, toId, componentId
    // with each row representing a unique conflict pair
    conflictsDf.alias(from).withColumnRenamed("id", fromId)
      .join(conflictsDf.alias(to).withColumnRenamed("id", toId), Seq(componentId, systemId), "outer")
      .filter(col(fromId) < col(toId))
      .drop(systemId)
  }

  private def getComponentsWithConflicts(conflictsDf: DataFrame): Set[String] = {
    val conflictComponents = conflictsDf.select(componentId).rdd.map(r => r.getAs[String](0)).collect
    Set(conflictComponents:_*)
  }

  private def generateInconsistencyReportDf(vertexDf: DataFrame, conflictIdsDf: DataFrame): DataFrame = {
    val flipedConflictIdsDf = conflictIdsDf
      .drop(componentId)
      .withColumnRenamed(fromId, "temp")
      .withColumnRenamed(toId, fromId)
      .withColumnRenamed("temp", toId)

    val otherVertices = conflictIdsDf
      .drop(componentId)
      .union(flipedConflictIdsDf)
      .withColumnRenamed(fromId, "id")
      .withColumnRenamed(toId, otherIdVId)
    
    vertexDf
      .drop(vertexType)
      .join(otherVertices, Seq("id"), "inner")
  }

  private def initiateGraphAndRunPregel(graph: Graph[(String, String, String, String, Long), String], maxComponentSize: Int //
      ): Graph[PregelVertexAttr, String] = {

    val initialGraph: Graph[PregelVertexAttr, String] = graph.mapVertices {
      case (id, (_, system, value, componentId, toVertex)) =>
        var initMessage = false
        var waitingFor = -1L
        if (toVertex != null) {
          initMessage = true
          waitingFor = toVertex
        }
        val vertexHash = s"$system-$value"
        new PregelVertexAttr(waitingFor, List(), initMessage, List(), system, componentId, vertexHash)
    }

    runPregel(initialGraph, maxComponentSize)
  }

  private def runPregel(graph: Graph[PregelVertexAttr, String], maxIter: Int): Graph[PregelVertexAttr, String] = {
    val emptyMessageList: List[VertexMessage] = List()
    graph.pregel(emptyMessageList, maxIter, EdgeDirection.Either)(
      // Vertex Program
      (id, attr, incomingMessages) => {
        val systemId: String = attr.systemId
        var messageList: List[VertexMessage] = List()
        var initMessage: Boolean = attr.initMessage
        if (initMessage) {
          val m = new VertexMessage(id, id, systemId, attr.vertexHash, List(), mutable.Set())
          messageList = messageList :+ m
          initMessage = false
        }

        var newFoundPaths = attr.foundPaths
        incomingMessages.foreach(message => {
          // Add the latest pair to the path
          var path = message.path
          val fromVertexHash = message.fromHash
          val toVertexHash = attr.vertexHash
          val pairHash = fromVertexHash + toVertexHash
          var pairType: String = ""
          if (path.size % 2 == 1) {
            pairType = message.fromSystem + "-" + systemId
            path = path :+ PathPair(message.from, id, pairType, pairHash)
          } else {
            pairType = systemId + "-" + message.fromSystem
            path = path :+ PathPair(id, message.from, pairType, pairHash)
          }
          val newMessage = new VertexMessage(message.src, id, systemId, toVertexHash, path, mutable.Set())

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
        new PregelVertexAttr(attr.waitingFor, messageList, initMessage, newFoundPaths, //
          systemId, attr.componentId, attr.vertexHash)
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
      (a, b) => a ++: b
    )
  }

}

case class PregelVertexAttr(waitingFor: Long, messageList: List[VertexMessage], initMessage: Boolean, //
                            foundPaths: List[VertexMessage], systemId: String, componentId: String, //
                            vertexHash: String) extends Serializable {

  override def toString: String = {
    s"waitingFor: $waitingFor, foundPaths: ${foundPaths.mkString}"
  }
}

case class VertexMessage(src: Long, from: Long, fromSystem: String, fromHash: String, //
                         path: List[PathPair], sent: mutable.Set[Long]) extends Serializable {

  override def toString: String = {
    s"src: $src, path: ${path.mkString}"
  }
}

case class PathPair(docVId: Long, idVId: Long, pairType: String, pairHash: String) extends Serializable {

  override def toString: String = {
    s"($pairType, $docVId, $idVId)"
  }
}
