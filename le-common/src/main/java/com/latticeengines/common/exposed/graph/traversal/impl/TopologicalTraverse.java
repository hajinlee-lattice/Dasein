package com.latticeengines.common.exposed.graph.traversal.impl;

import java.util.Collection;
import java.util.Stack;

import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;

public class TopologicalTraverse extends DepthFirstSearch {

    public void traverse(Collection<? extends GraphNode> nodes, Visitor visitor) {
        for (GraphNode node : nodes) {
            TopologicalVisitor topVisitor = new TopologicalVisitor(visitor);
            run(node, topVisitor, true);
            getSeenNodes().addAll(topVisitor.topologicalTrace);
        }
    }

    @Override
    public void preRun(Visitor visitor) {
    }

    @Override
    public void postRun(Visitor visitor) {
    }

    private static class TopologicalVisitor implements Visitor {

        public Stack<GraphNode> topologicalTrace = new Stack<>();
        private Visitor innerVisitor;

        public TopologicalVisitor(Visitor visitor) { innerVisitor = visitor; }

        @Override
        public void visit(Object o, VisitorContext ctx){
            topologicalTrace.push((GraphNode) o);
            innerVisitor.visit(o, ctx);
        }

    }

}