package com.latticeengines.common.exposed.graph;

import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.latticeengines.common.exposed.graph.traversal.impl.ReverseTopologicalTraverse;
import com.latticeengines.common.exposed.graph.traversal.impl.TopologicalTraverse;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;

public class TopologicalTraverseUnitTestNG {

    @Test(groups = "unit")
    public void topSort() {
        List<IntegerNode> nodes = constructGraph();

        // ========================================
        // source from root
        // ========================================
        TopologicalTraverse topTrav = new TopologicalTraverse();
        IntegerNodeVistor visitor = new IntegerNodeVistor();
        topTrav.traverse(nodes, visitor);

        Set<Integer> seenNums = new HashSet<>();
        for (Integer num : visitor.trace) {
            if (num.equals(1)) {
                Assert.assertTrue(seenNums.contains(2));
                Assert.assertTrue(seenNums.contains(3));
                Assert.assertTrue(seenNums.contains(4));
            }
            if (num.equals(3) || num.equals(4)) {
                Assert.assertTrue(seenNums.contains(5));
            }
            seenNums.add(num);
        }

        // ========================================
        // source from non-root
        // ========================================
        topTrav = new TopologicalTraverse();
        visitor = new IntegerNodeVistor();
        topTrav.traverse(nodes, visitor);

        System.out.println(visitor.trace);

        Assert.assertEquals(visitor.trace.size(), nodes.size());
        Assert.assertTrue(visitor.trace.contains(1));
        Assert.assertTrue(visitor.trace.contains(2));
        Assert.assertTrue(visitor.trace.contains(3));
        Assert.assertTrue(visitor.trace.contains(4));
        Assert.assertTrue(visitor.trace.contains(5));
        Assert.assertTrue(visitor.trace.contains(6));

        seenNums = new HashSet<>();
        for (Integer num : visitor.trace) {
            if (num.equals(1)) {
                Assert.assertTrue(seenNums.contains(2));
                Assert.assertTrue(seenNums.contains(3));
                Assert.assertTrue(seenNums.contains(4));
            }
            if (num.equals(3) || num.equals(4)) {
                Assert.assertTrue(seenNums.contains(5));
            }
            seenNums.add(num);
        }
    }

    @Test(groups = "unit")
    public void reverseTopTraverse() {
        List<IntegerNode> nodes = constructGraph();

        ReverseTopologicalTraverse reverse = new ReverseTopologicalTraverse();
        IntegerNodeVistor reverseVisitor = new IntegerNodeVistor();
        reverse.traverse(nodes, reverseVisitor);

        TopologicalTraverse forwards = new TopologicalTraverse();
        IntegerNodeVistor forwardsVisitor = new IntegerNodeVistor();
        forwards.traverse(nodes, forwardsVisitor);

        assertEquals(forwardsVisitor.trace, Lists.reverse(reverseVisitor.trace));
    }

    private List<IntegerNode> constructGraph() {
        IntegerNode node1 = new IntegerNode(1);
        IntegerNode node2 = new IntegerNode(2);
        IntegerNode node3 = new IntegerNode(3);
        IntegerNode node4 = new IntegerNode(4);
        IntegerNode node5 = new IntegerNode(5);
        IntegerNode node6 = new IntegerNode(6);

        node1.children.add(node2);
        node1.children.add(node3);
        node1.children.add(node4);

        node3.children.add(node5);
        node4.children.add(node5);

        return Arrays.asList(node1, node2, node3, node4, node5, node6);
    }

    @Test(groups = "unit")
    public void topTraverseWithCycle() {
        List<IntegerNode> nodes = constructCyclicGraph();

        // ========================================
        // traverse from root
        // ========================================
        TopologicalTraverse topTrav = new TopologicalTraverse();
        IntegerNodeVistor visitor = new IntegerNodeVistor();
        boolean exception = false;
        try {
            topTrav.traverse(nodes, visitor);
        } catch (IllegalArgumentException e) {
            exception = true;
        }
        Assert.assertTrue(exception);
        Assert.assertTrue(visitor.trace.size() <= 1);
    }


    private List<IntegerNode> constructCyclicGraph() {
        IntegerNode node1 = new IntegerNode(1);
        IntegerNode node2 = new IntegerNode(2);
        IntegerNode node3 = new IntegerNode(3);
        IntegerNode node4 = new IntegerNode(4);
        IntegerNode node5 = new IntegerNode(5);

        node1.children.add(node2);

        node1.children.add(node3);
        node3.children.add(node4);
        node4.children.add(node5);
        node5.children.add(node1);

        return Arrays.asList(node1, node2, node3, node4, node5);
    }

    private static class IntegerNodeVistor implements Visitor {
        public List<Integer> trace = new ArrayList<>();

        @Override
        public void visit(Object o, VisitorContext ctx) {
            if (o.getClass().equals(IntegerNode.class)) {
                trace.add(((IntegerNode) o).value);
            }
        }
    }

}
