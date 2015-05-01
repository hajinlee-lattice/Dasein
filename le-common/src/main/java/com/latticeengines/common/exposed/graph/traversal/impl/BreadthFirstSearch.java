package com.latticeengines.common.exposed.graph.traversal.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.graph.traversal.AbstractTraversalAlgorithm;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;

public class BreadthFirstSearch extends AbstractTraversalAlgorithm {

    private final Queue<GraphNode> queue = new LinkedList<>();
    private final Set<GraphNode> seenNodes = new HashSet<GraphNode>();

    @Override
    public void run(GraphNode node, Visitor visitor) {
        queue.offer(node);
        runSearch(node, visitor);
    }

    @Override
    public void runSearch(GraphNode node, Visitor visitor) {
        VisitorContext ctx = new VisitorContext();
        ctx.setProperty("parent", node);
        while (!queue.isEmpty()) {
            GraphNode n = queue.remove();
            seenNodes.add(n);
            preRun(visitor);
            n.accept(visitor, ctx);
            postRun(visitor);
            Collection<? extends GraphNode> children = n.getChildren();

            ctx = new VisitorContext();
            ctx.setProperty("parent", n);

            for (GraphNode child : children) {
                if (!seenNodes.contains(child)) {
                    queue.offer(child);
                }
            }
        }
    }


    @Override
    public void preRun(Visitor visitor) {
    }

    @Override
    public void postRun(Visitor visitor) {
    }

}