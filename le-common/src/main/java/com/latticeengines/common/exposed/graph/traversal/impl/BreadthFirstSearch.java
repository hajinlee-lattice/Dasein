package com.latticeengines.common.exposed.graph.traversal.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.graph.traversal.AbstractTraversalAlgorithm;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;

public class BreadthFirstSearch extends AbstractTraversalAlgorithm {

    private final Queue<Pair<GraphNode, GraphNode>> queue = new LinkedList<>();
    private final Set<GraphNode> seenNodes = new HashSet<GraphNode>();

    @Override
    public void run(GraphNode node, Visitor visitor) {
        runSearch(node, visitor);
    }

    @Override
    public void runSearch(GraphNode node, Visitor visitor) {
        queue.offer(Pair.<GraphNode, GraphNode> of(node, null));
        while (!queue.isEmpty()) {
            Pair<GraphNode, GraphNode> pair = queue.poll();
            GraphNode n = pair.getLeft();
            GraphNode parent = pair.getRight();
            seenNodes.add(n);
            VisitorContext ctx = new VisitorContext();
            ctx.setProperty("parent", parent);
            preRun(visitor);
            n.accept(visitor, ctx);
            postRun(visitor);
            Collection<? extends GraphNode> children = n.getChildren();
            for (GraphNode child : children) {
                if (!seenNodes.contains(child) && child != null) {
                    queue.offer(Pair.<GraphNode, GraphNode> of(child, n));
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
