package com.latticeengines.common.exposed.graph.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.graph.traversal.impl.DepthFirstSearch;
import com.latticeengines.common.exposed.graph.traversal.impl.TopologicalTraverse;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;

public class GraphUtils {
    public static <T> List<T> getAllOfType(GraphNode root, Class<T> clazz) {
        DepthFirstSearch search = new DepthFirstSearch();
        List<T> all = new ArrayList<>();
        if (root == null) {
            return all;
        }
        search.run(root, (object, ctx) -> {
            GraphNode node = (GraphNode) object;
            if (node != null && clazz.isInstance(node)) {
                all.add(clazz.cast(node));
            }
        });
        return all;
    }

    public static <T extends GraphNode> Map<T, Integer> getDepthMap(Collection<T> graph, Class<T> nodeClz) {
        TopologicalTraverse traverser = new TopologicalTraverse();
        DepthVisitor<T> vistor = new DepthVisitor<>(nodeClz);
        traverser.traverse(graph, vistor);
        return new HashMap<>(vistor.getDepthMap());
    }

    private static class DepthVisitor<T extends GraphNode> implements Visitor {
        private ConcurrentMap<T, Integer> depthMap = new ConcurrentHashMap<>();
        private final Class<T> nodeClz;

        DepthVisitor(Class<T> nodeClz) {
            this.nodeClz = nodeClz;
        }

        ConcurrentMap<T, Integer> getDepthMap() {
            return depthMap;
        }

        @Override
        public void visit(Object o, VisitorContext ctx) {
            T node = nodeClz.cast(o);
            if (CollectionUtils.isEmpty(node.getChildren())) {
                depthMap.put(node, 0);
            } else {
                int maxChildDepth = 0;
                for (GraphNode child: node.getChildren()) {
                    T childNode = nodeClz.cast(child);
                    maxChildDepth = Math.max(maxChildDepth, depthMap.get(childNode));
                }
                depthMap.put(node, maxChildDepth + 1);
            }
        }
    }

}
