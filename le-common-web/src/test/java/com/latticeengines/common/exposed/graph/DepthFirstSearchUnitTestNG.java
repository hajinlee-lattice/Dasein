package com.latticeengines.common.exposed.graph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.graph.traversal.impl.DepthFirstSearch;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;


public class DepthFirstSearchUnitTestNG {

    @Test(groups = "unit")
    public void dfs() {
        //========================================
        // construct a single-connected graph
        //========================================

        IntegerNode node1 = new IntegerNode(1);
        IntegerNode node2 = new IntegerNode(2);
        IntegerNode node3 = new IntegerNode(3);
        IntegerNode node4 = new IntegerNode(4);
        IntegerNode node5 = new IntegerNode(5);

        node1.children.add(node2);
        node1.children.add(node3);
        node1.children.add(node4);

        node3.children.add(node5);
        node4.children.add(node5);

        //========================================
        // traverse from root
        //========================================
        DepthFirstSearch dfs = new DepthFirstSearch();
        IntegerNodeVistor visitor = new IntegerNodeVistor();

        dfs.run(node1, visitor);

        Assert.assertEquals(visitor.trace.size(), 5);

        //========================================
        // reverse traverse from root
        //========================================

        dfs = new DepthFirstSearch();
        visitor = new IntegerNodeVistor();
        dfs.run(node1, visitor, true);

        Assert.assertEquals(visitor.trace.size(), 5);

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

        //========================================
        // traverse from non-root
        //========================================
        dfs = new DepthFirstSearch();
        visitor = new IntegerNodeVistor();
        dfs.run(node3, visitor);

        Assert.assertFalse(visitor.trace.contains(1));
        Assert.assertFalse(visitor.trace.contains(2));
        Assert.assertFalse(visitor.trace.contains(4));
    }


    @Test(groups = "unit")
    public void dfsWithCycle() {
        //========================================
        // construct a single-connected graph
        //========================================

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

        //========================================
        // traverse from root
        //========================================
        DepthFirstSearch dfs = new DepthFirstSearch();
        IntegerNodeVistor visitor = new IntegerNodeVistor();

        dfs.run(node1, visitor);

        Assert.assertFalse(dfs.getCycles().isEmpty());

        for (Cycle cycle : dfs.getCycles()) {
            Cycle expected = new Cycle(Arrays.asList(
                    (GraphNode) node4, node3, node1, node5
            ));
            Assert.assertEquals(cycle, expected);
            break;
        }

    }

    private static class IntegerNodeVistor implements Visitor {
        public List<Integer> trace = new ArrayList<>();

        @Override
        public void visit(Object o, VisitorContext ctx){
            if (o.getClass().equals(IntegerNode.class)) {
                trace.add(((IntegerNode) o).value);
            }
        }
    }


}
