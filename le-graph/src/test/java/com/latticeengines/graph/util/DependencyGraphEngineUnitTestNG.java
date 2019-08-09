package com.latticeengines.graph.util;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.graph.StringGraphNode;

public class DependencyGraphEngineUnitTestNG {

    @Test(groups = "unit")
    public void testDependencyLayers() {
        try (DependencyGraphEngine graphEngine = new DependencyGraphEngine()) {
            GraphTraversalSource g = graphEngine.getTraverser();

            StringGraphNode n1 = new StringGraphNode("1");
            StringGraphNode n2 = new StringGraphNode("2");
            StringGraphNode n3 = new StringGraphNode("3");
            StringGraphNode n4 = new StringGraphNode("4");
            StringGraphNode n5 = new StringGraphNode("5");
            StringGraphNode n6 = new StringGraphNode("6");
            StringGraphNode n7 = new StringGraphNode("7");
            StringGraphNode n8 = new StringGraphNode("8");
            StringGraphNode n9 = new StringGraphNode("9");

            n1.addChild(n2);
            n2.addChild(n3);
            n2.addChild(n4);
            n4.addChild(n5);
            n3.addChild(n6);
            n7.addChild(n8);

            List<GraphNode> graphNodes = Arrays.asList(n1, n2, n3, n4, n5, n6, n7, n8, n9);
            graphEngine.loadGraphNodes(graphNodes);

            Assert.assertEquals(g.V().toList().size(), 9);
            Assert.assertEquals(g.E().toList().size(), 6);

            List<Set<StringGraphNode>> layers = //
                    graphEngine.getDependencyLayers(StringGraphNode.class, Integer.MAX_VALUE);
            verifyLayerContains(layers.get(0), n5, n6, n8, n9);
            verifyLayerContains(layers.get(1), n3, n4, n7);
            verifyLayerContains(layers.get(2), n2);
            verifyLayerContains(layers.get(3), n1);

            layers = graphEngine.getDependencyLayers(StringGraphNode.class, 2);
            verifyLayerContains(layers.get(0), n5, n6, n8, n9);
            verifyLayerContains(layers.get(1), n1, n2, n3, n4, n7);
        }
    }

    @Test(groups = "unit")
    public void testCornerCases() {
        try (DependencyGraphEngine graphEngine = new DependencyGraphEngine()) {
            List<Set<StringGraphNode>> layers = //
                    graphEngine.getDependencyLayers(StringGraphNode.class, Integer.MAX_VALUE);
            Assert.assertNotNull(layers);
            Assert.assertEquals(layers.size(), 0);

            layers = graphEngine.getDependencyLayers(StringGraphNode.class, -1);
            Assert.assertNotNull(layers);
            Assert.assertEquals(layers.size(), 0);

            StringGraphNode n1 = new StringGraphNode("1");
            StringGraphNode n2 = new StringGraphNode("2");
            StringGraphNode n3 = new StringGraphNode("3");
            n1.addChild(n2);
            n2.addChild(n3);
            graphEngine.loadGraphNodes(Arrays.asList(n1, n2, n3));

            layers = graphEngine.getDependencyLayers(StringGraphNode.class, 0);
            Assert.assertNotNull(layers);
            Assert.assertEquals(layers.size(), 3);

            layers = graphEngine.getDependencyLayers(StringGraphNode.class, -1);
            Assert.assertNotNull(layers);
            Assert.assertEquals(layers.size(), 3);
        }
    }

    private void verifyLayerContains(Set<StringGraphNode> layer, StringGraphNode... nodes) {
        Assert.assertTrue(layer.containsAll(Arrays.asList(nodes)));
        Assert.assertEquals(layer.size(), nodes.length);
    }

}
