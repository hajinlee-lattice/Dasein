package com.latticeengines.common.exposed.graph.traversal;

import java.util.Stack;

import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;

public abstract class AbstractTraversalAlgorithm {

    protected Stack<VisitorContext> stack;

    public AbstractTraversalAlgorithm () {
        stack = new Stack<>();
    }

    public void run(GraphNode node, Visitor visitor) {
        VisitorContext ctx = new VisitorContext();
        ctx.setProperty("parent", null);
        stack.push(ctx);
        preRun(visitor);
        runSearch(node, visitor);
        postRun(visitor);
    }

    public abstract void runSearch(GraphNode node, Visitor visitor);

    public abstract void preRun(Visitor visitor);

    public abstract void postRun(Visitor visitor);
}
