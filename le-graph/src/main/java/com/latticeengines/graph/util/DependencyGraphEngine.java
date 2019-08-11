package com.latticeengines.graph.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.latticeengines.common.exposed.graph.GraphNode;

public class DependencyGraphEngine extends GraphEngine implements AutoCloseable {

    /**
     * First layer has no dependencies
     */
    public <T extends GraphNode> List<Set<T>> getDependencyLayers(Class<T> nodeClz, int maxLayers) {
        Map<T, Integer> depthMap = getDependencyDepth(nodeClz, null);
        return divideToLayers(depthMap, maxLayers);
    }

    public <T extends GraphNode> List<Set<T>> getDependencyLayersForSubDAG(Class<T> nodeClz, int maxLayers, //
                                                                             Collection<T> seed) {
        Map<T, Integer> depthMap = seed == null //
                ? getDependencyDepth(nodeClz, Collections.emptyList()) //
                : getDependencyDepth(nodeClz, seed);
        return divideToLayers(depthMap, maxLayers);
    }

    private <T extends GraphNode> List<Set<T>> divideToLayers(Map<T, Integer> depthMap, int maxLayers) {
        final int finalMaxLayers = maxLayers > 0 ? maxLayers : Integer.MAX_VALUE;
        int numLayers = depthMap.values().stream().mapToInt(Integer::intValue).max().orElse(0);
        if (numLayers > 0) {
            List<Set<T>> layers = new ArrayList<>();
            for (int i = 0; i < Math.min(finalMaxLayers, numLayers); i++) {
                layers.add(new HashSet<>());
            }
            depthMap.forEach((node, depth) -> {
                int layerIdx = Math.min(depth - 1, finalMaxLayers - 1);
                layers.get(layerIdx).add(node);
            });
            return layers;
        } else {
            return Collections.emptyList();
        }
    }

    private synchronized <G extends GraphNode> Map<G, Integer> getDependencyDepth(Class<G> nodeClz, Collection<G> seed) {
        verifyNoCycles();
        Collection<Vertex> seedVertices = null;
        if (seed != null) {
            seedVertices = getSubDAGVertices(seed, false);
        }
        GraphTraversal<Vertex, Vertex> root = seed == null ? g.V() : selectVertices(seedVertices);
        root.sideEffect(this::assignDepth) //
                .repeat(__.in(EDGE_LABEL).sideEffect(this::assignDepth)) //
                .emit().path().toList();
        Map<G, Integer> depMap = new HashMap<>();
        g.V().has(PROPERTY_DEPTH).toList().forEach(vertex -> {
            G graphNode = nodeClz.cast(vertex.property(PROPERTY_VALUE).value());
            Integer depth = (Integer) vertex.property(PROPERTY_DEPTH).value();
            depMap.put(graphNode, depth);
        });
        removeDepth();
        return depMap;
    }

    private void removeDepth() {
        g.V().properties(PROPERTY_DEPTH).drop().toList();
    }

    private void assignDepth(Traverser<Vertex> vertexTraverser) {
        vertexTraverser.get().property(PROPERTY_DEPTH, vertexTraverser.path().size());
    }

}
