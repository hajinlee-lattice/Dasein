package com.latticeengines.graphdb.entity.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.graph.GraphConstants;
import com.latticeengines.graphdb.BootstrapContext;
import com.latticeengines.graphdb.ConnectionManager;
import com.latticeengines.graphdb.GraphDbUtil;
import com.latticeengines.graphdb.entity.BaseGraphEntityManager;

public class BaseGraphEntityManagerImpl implements BaseGraphEntityManager {

    private static final Logger log = LoggerFactory.getLogger(BaseGraphEntityManagerImpl.class);

    @Inject
    private GraphDbUtil graphUtil;

    @Inject
    private ConnectionManager connectionManager;

    @Inject
    private BootstrapContext bootstrapContext;

    @Override
    public List<String> checkDirectVertexDependencies(String vertexId) throws Exception {
        log.info(String.format("vertexID = %s", vertexId));
        Cluster cluster = connectionManager.initCluster();
        try (GraphTraversalSource g = connectionManager.initTraversalSource(cluster)) {

            GraphTraversal<Vertex, Vertex> traversal = g.V(vertexId);
            Set<String> dependencies = new HashSet<>();
            traversal.inE().outV() //
                    .choose(__.properties(GraphConstants.BEHAVIOR_ON_DEP_CHECK_TRAVERSAL_KEY) //
                            .hasValue(GraphConstants.JUMP_DURING_DEP_CHECK), //
                            __.inE().outV().identity(), //
                            __.identity()) //
                    .forEachRemaining(dv -> {
                        dependencies.add(dv.id().toString());
                    });
            return new ArrayList<>(dependencies);
        } catch (Exception ex) {
            if (graphUtil.getIgnoreException()) {
                log.info("Ignoring exception till graph service is stable: " + ex.getMessage(), ex);
                return null;
            } else {
                throw ex;
            }
        } finally {
            connectionManager.closeCluster(cluster);

        }
    }

    @Override
    public List<List<String>> checkPotentialCircularDependencies(String inVertexId, String outVertexId)
            throws Exception {
        log.info(String.format("inVertexId = %s, outVertexId = %s", inVertexId, outVertexId));
        Cluster cluster = connectionManager.initCluster();
        try (GraphTraversalSource g = connectionManager.initTraversalSource(cluster)) {
            GraphTraversal<Vertex, Vertex> traversal = g.V(inVertexId);
            List<List<String>> paths = new ArrayList<>();
            traversal.repeat(__.outE().inV()) //
                    .until(__.hasId(outVertexId)) //
                    .path() //
                    .forEachRemaining(p -> {
                        List<Object> pathObjects = p.objects();
                        if (pathObjects != null && !pathObjects.isEmpty()) {
                            List<String> path = new ArrayList<>();
                            pathObjects.stream().filter(o -> o instanceof DetachedVertex).forEach(o -> {
                                path.add(((DetachedVertex) o).id().toString());
                            });
                            paths.add(path);
                        }
                    });
            return paths;
        } catch (Exception ex) {
            if (graphUtil.getIgnoreException()) {
                log.info("Ignoring exception till graph service is stable: " + ex.getMessage(), ex);
                return null;
            } else {
                throw ex;
            }
        } finally {
            connectionManager.closeCluster(cluster);
        }
    }

    @Override
    public boolean checkVertexExists(String vertexId) throws Exception {
        log.info(String.format("vertexId = %s", vertexId));
        if (!bootstrapContext.checkVertexExists(vertexId)) {
            Cluster cluster = connectionManager.initCluster();
            try (GraphTraversalSource g = connectionManager.initTraversalSource(cluster)) {
                boolean exists = g.V(vertexId).hasNext();
                if (exists) {
                    bootstrapContext.setVertexExists(vertexId);
                }
                return exists;
            } finally {
                connectionManager.closeCluster(cluster);

            }
        } else {
            log.info(String.format("Skip checking vertex existence during bootstrap model for vertexId = %s " //
                    + "as it already exists", vertexId));
            return true;
        }
    }

    @Override
    public boolean dropVertex(String vertexId, boolean failIfDependencyExist) throws Exception {
        log.info(String.format("vertexId = %s, failIfDependencyExist = %s", vertexId, failIfDependencyExist));
        Cluster cluster = connectionManager.initCluster();
        try (GraphTraversalSource g = connectionManager.initTraversalSource(cluster)) {
            GraphTraversal<Vertex, Vertex> traversal = g.V(vertexId);

            if (failIfDependencyExist) {
                boolean depExists = traversal.inE().hasNext();
                traversal = g.V(vertexId);

                if (depExists) {
                    List<String> dependencies = new ArrayList<>();
                    traversal.inE().outV().forEachRemaining(dv -> {
                        dependencies.add(dv.id().toString());
                    });
                    throw new RuntimeException(String.format("Deletion failed, Dependencies exists %s",
                            JsonUtils.serialize(dependencies)));
                }
            }

            traversal.drop().iterate();
            return true;
        } catch (Exception ex) {
            if (graphUtil.getIgnoreException()) {
                log.info("Ignoring exception till graph service is stable: " + ex.getMessage(), ex);
                return false;
            } else {
                throw ex;
            }
        } finally {
            connectionManager.closeCluster(cluster);

        }
    }

    @Override
    public boolean dropEdge(String inVertexId, String outVertexId, String edgeLabel) throws Exception {
        log.info(
                String.format("inVertexId = %s, outVertexId = %s, edgeLabel = %s", inVertexId, outVertexId, edgeLabel));
        Cluster cluster = connectionManager.initCluster();
        try (GraphTraversalSource g = connectionManager.initTraversalSource(cluster)) {
            GraphTraversal<Vertex, Edge> traversal = g.V(outVertexId).outE();
            if (StringUtils.isNotBlank(edgeLabel)) {
                traversal = traversal.hasLabel(edgeLabel);
            }
            traversal.as("e").inV().has(T.id, inVertexId).select("e").drop().iterate();
            return true;
        } catch (Exception ex) {
            if (graphUtil.getIgnoreException()) {
                log.info("Ignoring exception till graph service is stable: " + ex.getMessage(), ex);
                return false;
            } else {
                throw ex;
            }
        } finally {
            connectionManager.closeCluster(cluster);

        }
    }

    @Override
    public boolean updateVertexProperty(String vertexId, Map<String, String> addOrUpdateProperties,
            List<String> removeProperties) throws Exception {
        log.info(String.format("vertexID = %s", vertexId));
        Cluster cluster = connectionManager.initCluster();
        try (GraphTraversalSource g = connectionManager.initTraversalSource(cluster)) {
            GraphTraversal<Vertex, Vertex> traversal = g.V(vertexId).as("v");
            if (MapUtils.isNotEmpty(addOrUpdateProperties)) {
                for (String propKey : addOrUpdateProperties.keySet()) {
                    traversal = traversal.property( //
                            Cardinality.single, propKey, addOrUpdateProperties.get(propKey));
                }
            }

            if (CollectionUtils.isNotEmpty(removeProperties)) {
                traversal = traversal
                        .properties( //
                                removeProperties.toArray(new String[removeProperties.size()])) //
                        .drop().select("v");
            }

            traversal.iterate();
            return true;
        } catch (Exception ex) {
            if (graphUtil.getIgnoreException()) {
                log.info("Ignoring exception till graph service is stable: " + ex.getMessage(), ex);
                return false;
            } else {
                throw ex;
            }
        } finally {
            connectionManager.closeCluster(cluster);

        }
    }

    @Override
    public boolean addVertex(String vertexId, String label, Map<String, String> properties,
            Map<String, Map<String, Map<String, String>>> outgoingEdgesToVertices) throws Exception {
        log.info(String.format("vertexId = %s, label = %s, outgoingEdgesToVertices = %s", vertexId, label,
                outgoingEdgesToVertices == null ? outgoingEdgesToVertices
                        : JsonUtils.serialize(outgoingEdgesToVertices)));
        Cluster cluster = connectionManager.initCluster();
        try (GraphTraversalSource g = connectionManager.initTraversalSource(cluster)) {
            GraphTraversal<Vertex, Vertex> traversal = g.addV(label).property(T.id, vertexId).as("nid");

            if (MapUtils.isNotEmpty(properties)) {
                for (String propKey : properties.keySet()) {
                    traversal = traversal.property(propKey, properties.get(propKey));
                }
            }

            GraphTraversal<Vertex, Edge> midTraversal = null;

            // TODO - fix bug in transaction boundary
            if (MapUtils.isNotEmpty(outgoingEdgesToVertices)) {
                boolean isFirst = midTraversal == null;

                for (String outVid : outgoingEdgesToVertices.keySet()) {
                    String edgeLabel = outgoingEdgesToVertices.get(outVid).keySet().iterator().next();
                    if (isFirst) {
                        midTraversal = traversal //
                                .V(outVid).as(outVid) //
                                .addE(edgeLabel) //
                                .from("nid").to(outVid);
                        isFirst = false;
                    } else {
                        midTraversal = midTraversal //
                                .V(outVid).as(outVid) //
                                .addE(edgeLabel) //
                                .from("nid").to(outVid);
                    }
                    Map<String, String> propMap = outgoingEdgesToVertices.get(outVid).get(edgeLabel);
                    if (MapUtils.isNotEmpty(propMap)) {
                        for (String propKey : propMap.keySet()) {
                            midTraversal = midTraversal.property(propKey, propMap.get(propKey));
                        }
                    }

                    traversal = midTraversal.outV();
                }
            }

            traversal.next();
            return true;
        } catch (Exception ex) {
            if (graphUtil.getIgnoreException()) {
                log.info("Ignoring exception till graph service is stable: " + ex.getMessage(), ex);
                return false;
            } else {
                throw ex;
            }
        } finally {
            connectionManager.closeCluster(cluster);

        }
    }

    @Override
    public boolean addEdge(String label, Map<String, String> properties, String inVertexID, String outVertexID)
            throws Exception {
        log.info(String.format("inVertexID = %s, label = %s, outVertexID = %s", inVertexID, label, outVertexID));
        Cluster cluster = connectionManager.initCluster();
        try (GraphTraversalSource g = connectionManager.initTraversalSource(cluster)) {
            GraphTraversal<Vertex, Edge> traversal = g.V(inVertexID).as("in") //
                    .V(outVertexID).as("out") //
                    .addE(label) //
                    .from("out").to("in") //
                    .as("edge");

            if (MapUtils.isNotEmpty(properties)) {
                for (String propKey : properties.keySet()) {
                    traversal = traversal.property(propKey, properties.get(propKey));
                }
            }

            traversal.next();
            return true;
        } catch (Exception ex) {
            if (graphUtil.getIgnoreException()) {
                log.info("Ignoring exception till graph service is stable: " + ex.getMessage(), ex);
                return false;
            } else {
                throw ex;
            }
        } finally {
            connectionManager.closeCluster(cluster);

        }
    }

    @Override
    public boolean checkEdgeExists(String label, String inVertexID, String outVertexID) throws Exception {
        log.info(String.format("inVertexID = %s, label = %s, outVertexID = %s", inVertexID, label, outVertexID));
        Cluster cluster = connectionManager.initCluster();
        try (GraphTraversalSource g = connectionManager.initTraversalSource(cluster)) {
            return g.V(outVertexID).outE(label).inV().hasId(inVertexID).hasNext();
        } finally {
            connectionManager.closeCluster(cluster);

        }
    }
}
