package com.latticeengines.common.exposed.graph.traversal.impl;

import java.util.Collection;
import java.util.List;
import java.util.Stack;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.Lists;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;

public class ReverseTopologicalTraverse extends TopologicalTraverse {

    @Override
    public void traverse(Collection<? extends GraphNode> nodes, Visitor visitor) {
        ReverseTopologicalVisitor reverseVisitor = new ReverseTopologicalVisitor();
        super.traverse(nodes, reverseVisitor);
        reverseVisitor.visited = Lists.reverse(reverseVisitor.visited);
        reverseVisitor.visited.stream().forEach(p -> visitor.visit(p.getLeft(), p.getRight()));
    }

    public class ReverseTopologicalVisitor implements Visitor {
        public List<Pair<GraphNode, VisitorContext>> visited = new Stack<>();

        @Override
        public void visit(Object o, VisitorContext ctx) {
            visited.add(new MutablePair<>((GraphNode) o, ctx));
        }
    }
}
