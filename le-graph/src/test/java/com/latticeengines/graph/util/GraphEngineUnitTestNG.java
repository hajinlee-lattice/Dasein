package com.latticeengines.graph.util;

import java.util.Arrays;
import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.graph.StringGraphNode;
import com.latticeengines.graph.exception.GraphProcessingException;

public class GraphEngineUnitTestNG {

    @Test(groups = "unit")
    public void testGraphCreation() {
        try (GraphEngine graphEngine = new GraphEngine()) {
            GraphTraversalSource g = graphEngine.getTraverser();
            List<Vertex> vertices = g.V().toList();
            Assert.assertEquals(vertices.size(), 0);
            g.addV("v1").property("val", "val1").next();
            g.addV("v2").property("val", "val2").next();
            Assert.assertEquals(g.V().toList().size(), 2);

            // get by label
            Vertex vertex = g.V().hasLabel("v1").next();
            Assert.assertNotNull(vertex);
            Assert.assertEquals(vertex.value("val"), "val1");

            // get by property
            vertex = g.V().has("val", "val1").next();
            Assert.assertNotNull(vertex);
            Assert.assertEquals(vertex.value("val"), "val1");

            Vertex vertex2 = g.V().hasLabel("v2").next();
            g.addE("to").from(vertex).to(vertex2).next();

            Assert.assertEquals(g.E().toList().size(), 1);
            Edge edge = g.V(vertex).outE("to").where(__.inV().is(vertex2)).next();
            Assert.assertNotNull(edge);
            Assert.assertEquals(edge.inVertex(), vertex2);
            Assert.assertEquals(edge.outVertex(), vertex);
        }
    }

    @Test(groups = "unit")
    public void testLoadGraphNodes() {
        try (GraphEngine graphEngine = new GraphEngine()) {
            GraphTraversalSource g = graphEngine.getTraverser();

            graphEngine.loadGraphNodes(null);
            Assert.assertEquals(g.V().toList().size(), 0);
            Assert.assertEquals(g.E().toList().size(), 0);

            StringGraphNode n1 = new StringGraphNode("1");
            StringGraphNode n2 = new StringGraphNode("2");
            StringGraphNode n3 = new StringGraphNode("3");
            StringGraphNode n4 = new StringGraphNode("4");
            StringGraphNode n5 = new StringGraphNode("5");
            StringGraphNode n6 = new StringGraphNode("6");
            List<GraphNode> graphNodes = Arrays.asList(n1, n2, n3, n4, n5, n6);

            n1.addChild(n2);
            n2.addChild(null);
            graphEngine.loadGraphNodes(graphNodes);
            Assert.assertEquals(g.V().toList().size(), 6);
            Assert.assertEquals(g.E().toList().size(), 1);

            n2.getChildren().clear();
            n2.addChild(n3);
            n2.addChild(n4);
            n4.addChild(n5);
            graphEngine.loadGraphNodes(graphNodes);

            Assert.assertEquals(g.V().toList().size(), 6);
            Assert.assertEquals(g.E().toList().size(), 4);
        }
    }

    @Test(groups = "unit")
    public void testCycleDetection() {
        try (GraphEngine graphEngine = new GraphEngine()) {
            List<Path> cycles= graphEngine.getCycles();
            Assert.assertNotNull(cycles);
            Assert.assertEquals(cycles.size(), 0);

            StringGraphNode n1 = new StringGraphNode("1");
            StringGraphNode n2 = new StringGraphNode("2");
            StringGraphNode n3 = new StringGraphNode("3");
            StringGraphNode n4 = new StringGraphNode("4");
            StringGraphNode n5 = new StringGraphNode("5");

            n1.addChild(n2);
            n2.addChild(n3);
            n2.addChild(n4);
            n4.addChild(n5);
            n5.addChild(n1);
            n3.addChild(n1);

            List<GraphNode> graphNodes = Arrays.asList(n1, n2, n3, n4, n5);
            graphEngine.loadGraphNodes(graphNodes);

            cycles= graphEngine.getCycles();
            Assert.assertEquals(cycles.size(), 2);

            Assert.assertThrows(GraphProcessingException.class, graphEngine::verifyNoCycles);
        }
    }

}
