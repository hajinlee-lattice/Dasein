package com.latticeengines.graph.util;

import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GraphEngineUnitTestNG {

    @Test(groups = "unit")
    public void testLoadGraphNodes() {
        try (GraphEngine graphEngine = new GraphEngine()) {
            GraphTraversalSource g = graphEngine.getTraverser();
            List<Vertex> vertices = g.V().toList();
            Assert.assertEquals(vertices.size(), 0);
            g.addV().property("val", "val1").next();
            g.addV().property("val", "val2").next();
            System.out.println(g.V().toList());
        }
    }

}
