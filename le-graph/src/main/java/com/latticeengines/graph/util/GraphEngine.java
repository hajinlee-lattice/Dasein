package com.latticeengines.graph.util;


import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.loops;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.unfold;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.graph.exception.GraphProcessingException;

public class GraphEngine implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(GraphEngine.class);

    protected static final String EDGE_LABEL = "to";

    protected static final String PROPERTY_ID = "id";
    protected static final String PROPERTY_VALUE = "val";
    protected static final String PROPERTY_DEPTH = "depth";

    private final Graph graph;
    protected final GraphTraversalSource g;

    GraphEngine() {
        graph = TinkerGraph.open();
        g = graph.traversal();
    }

    GraphTraversalSource getTraverser() {
        return g;
    }

    public void loadGraphNodes(@NotNull Collection<? extends GraphNode> graphNodes) {
        if (CollectionUtils.isNotEmpty(graphNodes)) {
            for (GraphNode graphNode : graphNodes) {
                addOrGetNode(graphNode);
            }
            for (GraphNode graphNode : graphNodes) {
                if (CollectionUtils.isNotEmpty(graphNode.getChildren())) {
                    for (GraphNode childNode : graphNode.getChildren()) {
                        if (childNode != null) {
                            addOrGetEdge(graphNode, childNode, EDGE_LABEL);
                        }
                    }
                }
            }
        }
    }

    private Vertex addOrGetNode(GraphNode graphNode) {
        int id = System.identityHashCode(graphNode);
        String clz = graphNode.getClass().getSimpleName();
        Vertex vertex;
        if (g.V().has(clz,PROPERTY_ID, id).hasNext()) {
            vertex = g.V().has(clz, PROPERTY_ID, id).next();
        } else {
            vertex = g.addV(clz).property(PROPERTY_ID, id).property(PROPERTY_VALUE, graphNode).next();
            log.info(String.format("Add a [%s] vertex v[%s]: %s", clz, vertex.id(), graphNode));
        }
        return vertex;
    }

    private Edge addOrGetEdge(GraphNode from, GraphNode to, String label) {
        Vertex vOut = addOrGetNode(from);
        Vertex vIn = addOrGetNode(to);
        Edge edge;
        if (g.V(vOut).outE(label).where(__.inV().is(vIn)).hasNext()) {
            edge = g.V(vOut).outE(label).where(__.inV().is(vIn)).next();
        } else {
            edge = g.addE(label).from(vOut).to(vIn).next();
            log.info(String.format("Add a [%s] edge from v[%s] to v[%s]", label, vOut.id(), vIn.id()));

        }
        return edge;
    }

    void verifyNoCycles() throws GraphProcessingException {
        List<Path> cycles = getCycles();
        if (CollectionUtils.isNotEmpty(cycles)) {
            List<String> pathStrList = cycles.stream().map(this::pathToStr).collect(Collectors.toList());
            String msg = String.format("Detect %d cycles: %s", //
                    pathStrList.size(), StringUtils.join(pathStrList, "; "));
            throw new GraphProcessingException(msg);
        }
    }

    private String pathToStr(Path path) {
        List<String> nodeExpressions = path.stream() //
                .map(pair -> {
                    Vertex vertex = (Vertex) pair.getValue0();
                    return vertex.property(PROPERTY_VALUE).value().toString();
                }) //
                .collect(Collectors.toList());
        return StringUtils.join(nodeExpressions, " -> ");
    }

    public List<Path> getCycles() {
        return g.V().as("a") //
                .repeat(__.out(EDGE_LABEL).simplePath()) //
                .emit(loops().is(P.gt(1))) //
                .out().where(P.eq("a")) //
                .path().dedup().by(unfold().order().by(T.id).dedup().fold()).toList();
    }

    /**
     * Extract edge-induced sub-graph from given seed nodes
     * This impl won't work if there are cycles
     * @param seed
     * @param forwardDirection: propagate to children (forward) or parent (backward)
     * @return
     */
    Collection<Vertex> getSubDAGVertices(Collection<? extends GraphNode> seed, boolean forwardDirection) {
        Set<Vertex> rootVertices = selectNodes(seed).toSet();
        Set<Vertex> derivedVertices;
        if (forwardDirection) {
            derivedVertices = selectNodes(seed).repeat(__.out(EDGE_LABEL)).emit().toSet();
        } else {
            derivedVertices = selectNodes(seed).repeat(__.in(EDGE_LABEL)).emit().toSet();
        }
        rootVertices.addAll(derivedVertices);
        return rootVertices;
    }

    private GraphTraversal<Vertex, Vertex> selectNodes(Collection<? extends GraphNode> seed) {
        return g.V().filter(vertexTraverser -> {
            GraphNode node = (GraphNode) vertexTraverser.get().property(PROPERTY_VALUE).value();
            return seed.contains(node);
        });
    }

    GraphTraversal<Vertex, Vertex> selectVertices(Collection<Vertex> vertices) {
        Object[] vertexIds = vertices.stream().map(Element::id).toArray();
        if (vertexIds.length > 0) {
            return g.V(vertexIds);
        } else {
            return g.V("none");
        }
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
