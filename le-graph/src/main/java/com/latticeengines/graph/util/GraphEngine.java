package com.latticeengines.graph.util;


import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphEngine implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(GraphEngine.class);

    private final Graph graph;
    private final GraphTraversalSource g;

    public GraphEngine() {
        graph = TinkerGraph.open();
        g = graph.traversal();
    }

    GraphTraversalSource getTraverser() {
        return g;
    }

    @Override
    public void close() {
        if (g != null) {
            try {
                g.close();
            } catch (Exception e) {
                 log.error("Failed to close graph traverser.", e);
            }
        }
        if (graph != null) {
            try {
                graph.close();
            } catch (Exception e) {
                log.error("Failed to close graph.", e);
            }
        }
    }

}
