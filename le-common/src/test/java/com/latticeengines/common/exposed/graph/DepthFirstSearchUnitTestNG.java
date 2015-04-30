package com.latticeengines.common.exposed.graph;

import java.util.ArrayList;
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

        //========================================
        // traverse from root
        //========================================
        DepthFirstSearch dfs = new DepthFirstSearch();
        IntegerNodeVistor visitor = new IntegerNodeVistor();

        dfs.run(node1, visitor);

        Set<Integer> seenNums = new HashSet<>();
        for (Integer num : visitor.trace) {
            if (num.equals(2) || num.equals(3) || num.equals(4)) {
                Assert.assertTrue(seenNums.contains(1));
            }
            if (num.equals(5)) {
                Assert.assertTrue(seenNums.contains(3));
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


    private class IntegerNode implements GraphNode {

        public Set<IntegerNode> children = new HashSet<>();
        public Integer value;

        public IntegerNode(Integer value) { this.value = value; }

        @Override
        public List<IntegerNode> getChildren() { return new ArrayList<>(children); }

        @Override
        public void accept(Visitor visitor, VisitorContext ctx) {
            visitor.visit(this, ctx);
        }

        @Override
        public boolean equals(Object that) {
            return that.getClass().equals(IntegerNode.class)
                    && this.value.equals(((IntegerNode) that).value);
        }

    }

    private class IntegerNodeVistor implements Visitor {
        public List<Integer> trace = new ArrayList<>();

        @Override
        public void visit(Object o, VisitorContext ctx){
            if (o.getClass().equals(IntegerNode.class)) {
                trace.add(((IntegerNode) o).value);
            }
        }
    }

}
