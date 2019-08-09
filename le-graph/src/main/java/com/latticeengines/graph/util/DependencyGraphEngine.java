package com.latticeengines.graph.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.latticeengines.common.exposed.graph.GraphNode;

public class DependencyGraphEngine extends GraphEngine implements AutoCloseable {

    /**
     * First layer has no dependencies
     */
    public <T extends GraphNode> List<Set<T>> getDependencyLayers(Class<T> nodeClz, int maxLayers) {
        Map<T, Integer> depthMap = getDependencyDepth(nodeClz);
        int numLayers = depthMap.values().stream().mapToInt(Integer::intValue).max().orElse(0);
        if (numLayers > 0) {
            List<Set<T>> layers = new ArrayList<>();
            for (int i = 0; i < Math.min(maxLayers, numLayers); i++) {
                layers.add(new HashSet<>());
            }
            depthMap.forEach((node, depth) -> {
                int layerIdx = Math.min(depth - 1, maxLayers - 1);
                layers.get(layerIdx).add(node);
            });
            return layers;
        } else {
            return Collections.emptyList();
        }
    }

    private <T extends GraphNode> Map<T, Integer> getDependencyDepth(Class<T> nodeClz) {
        verifyNoCycles();
        g.V().sideEffect(this::assignDepth) //
                .repeat(__.in(EDGE_LABEL).sideEffect(this::assignDepth)) //
                .emit().path().toList();
        List<Vertex> vertexList = g.V().toList();
        Map<T, Integer> depMap = new HashMap<>();
        vertexList.forEach(vertex -> {
            T graphNode = nodeClz.cast(vertex.property(PROPERTY_VALUE).value());
            Integer depth = (Integer) vertex.property(PROPERTY_DEPTH).value();
            depMap.put(graphNode, depth);
        });
        return depMap;
    }

    private void assignDepth(Traverser<Vertex> vertexTraverser) {
        vertexTraverser.get().property(PROPERTY_DEPTH, vertexTraverser.path().size());
    }

}
