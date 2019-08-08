package com.latticeengines.graphdb.entity;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.graph.EdgeCreationRequest;
import com.latticeengines.domain.exposed.graph.EdgeDeletionRequest;
import com.latticeengines.domain.exposed.graph.VertexCreationRequest;
import com.latticeengines.domain.exposed.graph.VertexDeletionRequest;

public interface GraphEntityManager {

    void addVertex(String customerSpace, String overrideEnv, String overrideVersion, String overrideTenant,
            VertexCreationRequest request) throws Exception;

    boolean addEdge(String customerSpace, String overrideEnv, String overrideVersion, String overrideTenant,
            List<EdgeCreationRequest> request) throws Exception;

    List<Map<String, String>> checkDirectVertexDependencies(String customerSpace, String overrideEnv,
            String overrideVersion, String overrideTenant, String objectId, String type) throws Exception;

    List<List<Map<String, String>>> checkPotentialCircularDependencies(String customerSpace, String overrideEnv,
            String overrideVersion, String overrideTenant, String fromObjectId, String fromObjectType,
            String toObjectId, String toObjectType) throws Exception;

    boolean dropVertex(String customerSpace, String overrideEnv, String overrideVersion, String overrideTenant,
            VertexDeletionRequest request) throws Exception;

    boolean dropEdge(String customerSpace, String overrideEnv, String overrideVersion, String overrideTenant,
            List<EdgeDeletionRequest> request) throws Exception;

    boolean updateVertexProperty(String customerSpace, String overrideEnv, String overrideVersion,
            String overrideTenant, String vertexId, Map<String, String> addOrUpdateProperties,
            List<String> removeProperties) throws Exception;

    boolean checkVertexExists(String customerSpace, String overrideEnv, String overrideVersion, String overrideTenant,
            String objectId, String objectType) throws Exception;

    boolean checkEdgeExists(String customerSpace, String overrideEnv, String overrideVersion, String overrideTenant,
            String edgeType, String fromObjectID, String fromObjectType, String toObjectID, String toObjectType)
            throws Exception;
}
