package com.latticeengines.common.exposed.graph.traversal.impl;

import java.util.ArrayList;
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

    private final Stack<GraphNode> seenNodes;
    private final Set<Cycle> cycles;

    public DepthFirstSearch () {
        super();
        seenNodes = new Stack<>();
        cycles = new HashSet<>();
    }

    @Override
    public void runSearch(GraphNode node, Visitor visitor) {
        node.accept(visitor, stack.peek());
        List<? extends GraphNode> children = node.getChildren();
        VisitorContext ctx = new VisitorContext();
        ctx.setProperty("parent", node);

        stack.push(ctx);
        seenNodes.push(node);
        for (GraphNode child : children) {
            if (!seenNodes.contains(child)) {
                runSearch(child, visitor);
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
    }

    @Override
    public void preRun(Visitor visitor) {
    }

    @Override
    public void postRun(Visitor visitor) {
    }

    public Set<Cycle> getCycles() {
        return cycles;
    }

}