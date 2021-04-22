package com.latticeengines.spark.exposed.job.graph

import com.latticeengines.domain.exposed.spark.graph.AssignEntityIdsJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, lit, max, udf, row_number}
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
  private val src = "src"
  private val dst = "dst"
  private val tieBreakingProcess = "TieBreakingProcess"
  private val garbageCollector = "GarbageCollector"
  private val resolutionStrategy = "ResolutionStrategy"

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

    val conflictOutputs: List[DataFrame] = generateConflictOutputs(vertices)
    val conflictIdsDf: DataFrame = conflictOutputs.head // for TieBreakingProcess
    val gcComponents: DataFrame = conflictOutputs.last // for GarbageCollecting

    var tieBreakingVertices: DataFrame = getTieBreakingVertices(conflictIdsDf, gcComponents, vertices)

    val graph = generateFilteredGraph(vertices, edges, tieBreakingVertices, conflictIdsDf)
    val pregelGraph: Graph[PregelVertexAttr, String] = initiateGraphAndRunPregel(graph, maxComponentSize)
    val edgesToRemove: RDD[Row] = calculateEdgesToRemove(spark, pregelGraph, edgeRank)
    val updatedEdgesDf: DataFrame = removeConflictingEdges(spark, edges, edgesToRemove, gcComponents, vertices)

    val entityIdsDf = generateEntityIdsDf(updatedEdgesDf, gcComponents, vertices)
    val verticesWithEntityIdsDf: DataFrame = vertices.join(entityIdsDf, Seq("id"), joinType="left")

    val inconsistencyReportDf: DataFrame = generateInconsistencyReportDf(verticesWithEntityIdsDf, conflictIdsDf, gcComponents)

    lattice.output = List(verticesWithEntityIdsDf, updatedEdgesDf, inconsistencyReportDf)
  }

  private def removeConflictingEdges(spark: SparkSession, edges: DataFrame, //
      edgesToRemove: RDD[Row], gcComponents: DataFrame, vertices: DataFrame): DataFrame = {

    val gcVertices: DataFrame = vertices
      .filter(col(vertexType) === docV)
      .withColumnRenamed("id", src)
      .join(gcComponents, Seq(componentId), joinType="right")
      .select(src)

    val edgesToRemoveDf = spark.createDataFrame(edgesToRemove, edgePairSchema)

    val edgesAfterTieBreaking = edges
      .join(edgesToRemoveDf, Seq(src, dst), joinType="leftanti")
    
    edgesAfterTieBreaking
      .join(gcVertices, Seq(src), joinType="leftanti")
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

  private def generateFilteredGraph(vertices: DataFrame, edges: DataFrame, tieBreakingVertices: DataFrame, //
      conflictIdsDf: DataFrame):Graph[(String, String, String, String, Long), String] = {

    // tieBreakingVertices has one column "id"
    val filteredEdges = edges
      .join(tieBreakingVertices.withColumnRenamed("id", src), Seq(src), joinType="right")

    val erdd: RDD[Edge[String]] = filteredEdges
      .rdd.map(row => Edge(row.getAs[VertexId](0), row.getAs[VertexId](1), row.getAs[String](2)))

    // conflictIdsDf has componentId, fromId, toId
    val verticesWithToId: DataFrame = vertices
      .join(conflictIdsDf.drop(componentId).withColumnRenamed(fromId, "id"), Seq("id"), joinType="left")

    val vrdd: RDD[(VertexId,
      (
        String /* type */, //
        String /* systemID */, //
        String /* VertexValue */, //
        String /* ComponentID */,
        Long /* toID */ //
      ))] = verticesWithToId
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

  private def generateEntityIdsDf(edgesDf: DataFrame, gcComponents: DataFrame, vertices: DataFrame): DataFrame = {
    val verticesWithoutGc: DataFrame = vertices
      .join(gcComponents, Seq(componentId), joinType="leftanti")

    val graphFrame = GraphFrame(verticesWithoutGc, edgesDf)
    val entityDf: DataFrame = graphFrame.connectedComponents.run()

    val entityIdUDF = udf(() => java.util.UUID.randomUUID.toString)
    val entityIds: DataFrame = entityDf.select(component).distinct.withColumn(entityId, entityIdUDF())

    entityDf.join(entityIds, Seq(component), joinType = "inner").select("id", entityId)
  }

  private def generateConflictOutputs(vertices: DataFrame): List[DataFrame] = {
    val w = Window.partitionBy(componentId, systemId)
    val countedVertices: DataFrame = vertices
      .withColumn(countCol, count(systemId).over(w))

    // Components with more than 4 conflicting values
    // We only need one row per component
    val rowNumber = "rowNumber"
    val gcComponents: DataFrame = countedVertices
      .filter(col(countCol) > 4)
      .withColumn(rowNumber, row_number.over(Window.partitionBy(componentId).orderBy("id")))
      .filter(col(rowNumber) === 1)
      .select(componentId)

    // Only contains rows that are conflicting IdVs
    val conflictsDf: DataFrame = countedVertices
      .filter(col(vertexType) === idV && col(countCol) > 1 && col(countCol) < 5)
      .select("id", systemId, componentId)

    // conflictIdsDf columns: fromId, toId, componentId
    // with each row representing a unique conflict pair
    val conflictIdsDf: DataFrame = conflictsDf.alias(from).withColumnRenamed("id", fromId)
      .join(conflictsDf.alias(to).withColumnRenamed("id", toId), Seq(componentId, systemId), joinType="outer")
      .filter(col(fromId) < col(toId))
      .drop(systemId)

    List(conflictIdsDf, gcComponents)
  }

  private def getTieBreakingVertices(conflictsDf: DataFrame, gcComponents: DataFrame, //
      vertices: DataFrame): DataFrame = {

    val verticesAfterGc = vertices
      .join(gcComponents.drop("id"), Seq(componentId), joinType="leftanti")

    verticesAfterGc
      .filter(col(vertexType) === docV)
      .join(conflictsDf, Seq(componentId), joinType="right")
      .select("id")
  }

  private def generateInconsistencyReportDf(vertexDf: DataFrame, conflictIdsDf: DataFrame, //
      gcComponents: DataFrame): DataFrame = {

    val flipedConflictIdsDf = conflictIdsDf
      .withColumnRenamed(fromId, "temp")
      .withColumnRenamed(toId, fromId)
      .withColumnRenamed("temp", toId)

    val allTieBreakingVertices = conflictIdsDf
      .union(flipedConflictIdsDf)
      .withColumnRenamed(fromId, "id")
      .withColumn(resolutionStrategy, lit(tieBreakingProcess))
      .select("id", resolutionStrategy)

    val tieBreakingReport = vertexDf.join(allTieBreakingVertices, Seq("id"), joinType="inner")

    val gcVertices = vertexDf
      .join(gcComponents, Seq(componentId), joinType="right")
      .withColumn(resolutionStrategy, lit(garbageCollector))
      .select("id", resolutionStrategy)

    val gcReport = vertexDf.join(gcVertices, Seq("id"), joinType="inner")

    tieBreakingReport.union(gcReport)
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
