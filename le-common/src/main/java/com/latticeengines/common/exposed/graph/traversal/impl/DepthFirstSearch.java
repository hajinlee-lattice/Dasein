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

    private Set<GraphNode> seenNodes;
    private Stack<GraphNode> currentPath;
    private final Set<Cycle> cycles;

    public DepthFirstSearch () {
        super();
        seenNodes = new HashSet<>();
        currentPath = new Stack<>();
        cycles = new HashSet<>();
    }

    public void run(GraphNode node, Visitor visitor, boolean reverse) {
        currentPath = new Stack<>();
        if (reverse) {
            VisitorContext ctx = new VisitorContext();
            ctx.setProperty("parent", null);
            stack.push(ctx);
            preRun(visitor);
            runSearch(node, visitor, true);
            postRun(visitor);
        } else {
            run(node, visitor);
        }
    }

    @Override
    public void runSearch(GraphNode node, Visitor visitor) {
        runSearch(node, visitor, false);
    }

    private void runSearch(GraphNode node, Visitor visitor, boolean reverse) {
        if (seenNodes.contains(node)) return;

        VisitorContext originalCtx = stack.peek();
        if (!reverse) {
            node.accept(visitor, originalCtx);
        }

        Collection<? extends GraphNode> children = node.getChildren();
        VisitorContext ctx = new VisitorContext();
        ctx.setProperty("parent", node);

        stack.push(ctx);
        currentPath.push(node);
        if (children == null) {
            return;
        }
        for (GraphNode child : children) {
            if (child == null) {
                continue;
            }
            if (!currentPath.contains(child) && !seenNodes.contains(child)) {
                runSearch(child, visitor, reverse);
            }
            else if (currentPath.contains(child)) {
                List<GraphNode> cycle = new ArrayList<>();
                for (int i = currentPath.size() - 1; i >= 0; i--) {
                    GraphNode el = currentPath.get(i);
                    cycle.add(el);
                    if (el.equals(child)) {
                        break;
                    }
                }
                cycles.add(new Cycle(cycle));
            }
        }
        stack.pop();
        currentPath.pop();
        seenNodes.add(node);

        if (reverse && cycles.isEmpty()) {
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

    protected Set<GraphNode> getSeenNodes() { return seenNodes; }

    protected void setSeenNodes(Set<GraphNode> seenNodes) { this.seenNodes = seenNodes; }

}
