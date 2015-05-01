package com.latticeengines.common.exposed.graph.traversal.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import com.latticeengines.common.exposed.graph.Cycle;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.graph.traversal.AbstractTraversalAlgorithm;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;

public class DepthFirstSearch extends AbstractTraversalAlgorithm {

    private Stack<GraphNode> seenNodes;
    private final Set<Cycle> cycles;

    public DepthFirstSearch () {
        super();
        seenNodes = new Stack<>();
        cycles = new HashSet<>();
    }

    public void run(GraphNode node, Visitor visitor, boolean reverse) {
        if (reverse) {
            VisitorContext ctx = new VisitorContext();
            ctx.setProperty("parent", null);
            stack.push(ctx);
            preRun(visitor);
            runSearch(node, visitor, reverse);
            postRun(visitor);
        } else {
            run(node, visitor);
        }
    }

    @Override
    public void runSearch(GraphNode node, Visitor visitor) {
        runSearch(node, visitor, false);
    }

    public void runSearch(GraphNode node, Visitor visitor, boolean reverse) {
        if (seenNodes.contains(node)) return;

        VisitorContext originalCtx = stack.peek();
        if (!reverse) {
            node.accept(visitor, originalCtx);
        }

        Collection<? extends GraphNode> children = node.getChildren();
        VisitorContext ctx = new VisitorContext();
        ctx.setProperty("parent", node);

        stack.push(ctx);
        seenNodes.push(node);
        for (GraphNode child : children) {
            if (!seenNodes.contains(child)) {
                runSearch(child, visitor, reverse);
            }
            else {
                List<GraphNode> cycle = new ArrayList<>();
                for (int i = seenNodes.size() - 1; i >= 0; i--) {
                    GraphNode el = seenNodes.get(i);
                    cycle.add(el);
                    if (el.equals(child)) {
                        break;
                    }
                }
                cycles.add(new Cycle(cycle));
            }
        }
        stack.pop();
        seenNodes.pop();

        if (reverse) {
            node.accept(visitor, originalCtx);
        }
    }

    @Override
    public void preRun(Visitor visitor) {
    }

    @Override
    public void postRun(Visitor visitor) {
    }

    public Set<Cycle> getCycles() { return cycles; }

    protected Stack<GraphNode> getSeenNodes() { return seenNodes; }

    protected void setSeenNodes(Stack<GraphNode> seenNodes) { this.seenNodes = seenNodes; }

}