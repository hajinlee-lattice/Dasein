package com.latticeengines.spark.exposed.job.graph

import com.latticeengines.domain.exposed.spark.SparkJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.graphframes.GraphFrame

class GraphPageRankJob extends AbstractSparkJob[SparkJobConfig] {
  override def runJob(spark: SparkSession, lattice: LatticeContext[SparkJobConfig]): Unit = {
    // Create a GraphFrame
    val vertices: DataFrame = lattice.input.head
    val edges: DataFrame = lattice.input(1)
    val g = GraphFrame(vertices, edges)

    // Query: Get in-degree of each vertex.
    g.inDegrees.show()

    // Query: Count the number of "follow" connections in the graph.
    g.edges.filter("relationship = 'follow'").count()

    // Run PageRank algorithm, and show results.
    val results = g.pageRank.resetProbability(0.01).maxIter(20).run()
    results.vertices.select("id", "pagerank").show()

    lattice.output = results.vertices :: results.edges :: Nil
  }
}
