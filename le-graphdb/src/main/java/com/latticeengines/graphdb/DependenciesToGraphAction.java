package com.latticeengines.graphdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.graph.EdgeCreationRequest;
import com.latticeengines.domain.exposed.graph.EdgeDeletionRequest;
import com.latticeengines.domain.exposed.graph.GraphConstants;
import com.latticeengines.domain.exposed.graph.ParsedDependencies;
import com.latticeengines.domain.exposed.graph.VertexCreationRequest;
import com.latticeengines.domain.exposed.graph.VertexDeletionRequest;
import com.latticeengines.graphdb.entity.GraphEntityManager;

@Component
public class DependenciesToGraphAction {

    @Inject
    private GraphEntityManager graphEntityManager;

    @Inject
    private ConnectionManager connectionManager;

    public void beginThreadLocalCluster() {
        connectionManager.initThreadLocalCluster();
    }

    public void closeThreadLocalCluster() {
        connectionManager.cleanupThreadLocalCluster();
    }

    public void createVertex(String tenantId, ParsedDependencies parsedDependencies, String vertexId, String vertexType)
            throws Exception {
        createVertex(tenantId, parsedDependencies, vertexId, vertexType, null, null);
    }

    public void createVertex(String tenantId, ParsedDependencies parsedDependencies, String vertexId, String vertexType,
            Map<String, String> vertexProperties, List<Map<String, String>> edgeProperties) throws Exception {
        if (!graphEntityManager.checkVertexExists(tenantId, null, null, null, vertexId, vertexType)) {
            VertexCreationRequest request = new VertexCreationRequest();
            request.setObjectId(vertexId);
            request.setType(vertexType);
            if (vertexProperties == null) {
                vertexProperties = new HashMap<>();
            }
            vertexProperties.put(GraphConstants.TENANT_ID_PROP_KEY, tenantId);
            request.setProperties(vertexProperties);
            if (parsedDependencies != null //
                    && CollectionUtils.isNotEmpty(parsedDependencies.getAddDependencies())) {
                Map<String, Map<String, Map<String, String>>> outgoingEdgesToVertices = new HashMap<>();
                Map<String, String> outgoingVertexTypes = new HashMap<>();
                final AtomicInteger idx = new AtomicInteger(0);

                parsedDependencies.getAddDependencies().stream() //
                        .forEach(a -> {
                            Map<String, Map<String, String>> edgeInfo = new HashMap<>();
                            int index = idx.get();
                            idx.set(index + 1);
                            Map<String, String> propMap = null;
                            if (CollectionUtils.isNotEmpty(edgeProperties)
                                    && MapUtils.isNotEmpty(edgeProperties.get(index))) {
                                propMap = edgeProperties.get(index);
                            } else {
                                propMap = new HashMap<>();
                            }
                            propMap.put(GraphConstants.TENANT_ID_PROP_KEY, tenantId);
                            edgeInfo.put(a.getRight(), propMap);
                            outgoingEdgesToVertices.put(a.getLeft(), edgeInfo);

                            outgoingVertexTypes.put(a.getLeft(), a.getMiddle());
                        });

                request.setOutgoingEdgesToVertices(outgoingEdgesToVertices);
                request.setOutgoingVertexTypes(outgoingVertexTypes);
            }
            graphEntityManager.addVertex(tenantId, null, null, null, request);
        }
    }

    public Map<Pair<Pair<String, String>, Pair<String, String>>, List<List<Map<String, String>>>> addEdges(
            String tenantId, ParsedDependencies parsedDependencies, String vertexId, String vertexType)
            throws Exception {
        return addEdges(tenantId, parsedDependencies, vertexId, vertexType, null);
    }

    public Map<Pair<Pair<String, String>, Pair<String, String>>, List<List<Map<String, String>>>> addEdges(
            String tenantId, ParsedDependencies parsedDependencies, String vertexId, String vertexType,
            List<Map<String, String>> edgeProperties) throws Exception {
        Map<Pair<Pair<String, String>, Pair<String, String>>, List<List<Map<String, String>>>> potentialCircularDependencies = new HashMap<>();
        if (CollectionUtils.isNotEmpty(parsedDependencies.getAddDependencies())) {

            parsedDependencies.getAddDependencies().stream() //
                    .forEach(ad -> {
                        List<List<Map<String, String>>> paths = null;
                        try {
                            paths = graphEntityManager.checkPotentialCircularDependencies(tenantId, null, null, null,
                                    vertexId, vertexType, ad.getLeft(), ad.getMiddle());
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }

                        if (CollectionUtils.isNotEmpty(paths)) {
                            potentialCircularDependencies
                                    .put(new ImmutablePair<>(new ImmutablePair<>(vertexId, vertexType),
                                            new ImmutablePair<>(ad.getLeft(), ad.getMiddle())), paths);
                        }
                    });

            if (MapUtils.isEmpty(potentialCircularDependencies)) {
                List<EdgeCreationRequest> addEdgeRequest = new ArrayList<>();
                final AtomicInteger idx = new AtomicInteger(0);

                parsedDependencies.getAddDependencies().stream().forEach(ad -> {
                    int index = idx.get();
                    idx.set(index + 1);
                    Map<String, String> propMap = null;
                    if (CollectionUtils.isNotEmpty(edgeProperties) && MapUtils.isNotEmpty(edgeProperties.get(index))) {
                        propMap = edgeProperties.get(index);
                    } else {
                        propMap = new HashMap<>();
                    }
                    propMap.put(GraphConstants.TENANT_ID_PROP_KEY, tenantId);
                    EdgeCreationRequest e = new EdgeCreationRequest();
                    e.setFromObjectID(vertexId);
                    e.setFromObjectType(vertexType);
                    e.setToObjectID(ad.getLeft());
                    e.setToObjectType(ad.getMiddle());
                    e.setType(ad.getRight());
                    e.setProperties(propMap);
                    addEdgeRequest.add(e);
                });
                graphEntityManager.addEdge(tenantId, null, null, null, addEdgeRequest);
            }
        }
        return potentialCircularDependencies;
    }

    public void dropEdges(String tenantId, ParsedDependencies parsedDependencies, String vertexId, String vertexType)
            throws Exception {
        if (CollectionUtils.isNotEmpty(parsedDependencies.getRemoveDependencies())) {
            List<EdgeDeletionRequest> removeEdgeRequest = new ArrayList<>();
            parsedDependencies.getRemoveDependencies().stream().forEach(rd -> {
                EdgeDeletionRequest e = new EdgeDeletionRequest();
                e.setFromObjectID(vertexId);
                e.setFromObjectType(vertexType);
                e.setToObjectID(rd.getLeft());
                e.setToObjectType(rd.getMiddle());
                e.setType(rd.getRight());
                removeEdgeRequest.add(e);
            });
            graphEntityManager.dropEdge(tenantId, null, null, null, removeEdgeRequest);
        }
    }

    public void deleteVertex(String tenantId, String vertexId, String vertexType) throws Throwable {
        VertexDeletionRequest request = new VertexDeletionRequest();
        request.setObjectId(vertexId);
        request.setType(vertexType);
        request.setForceDelete(false);
        graphEntityManager.dropVertex(tenantId, null, null, null, request);
    }

    public List<Map<String, String>> checkDirectDependencies(String tenantId, String vertexId, String vertexType)
            throws Exception {
        return graphEntityManager.checkDirectVertexDependencies(tenantId, null, null, null, vertexId, vertexType);
    }

    public List<List<Map<String, String>>> checkPotentialCircularDependencies(String tenantId, String fromVertexId,
            String fromVertexType, String toVertexId, String toVertexType) throws Exception {
        return graphEntityManager.checkPotentialCircularDependencies(tenantId, null, null, null, fromVertexId,
                fromVertexType, toVertexId, toVertexType);
    }
}
