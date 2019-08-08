package com.latticeengines.graphdb.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.graph.EdgeCreationRequest;
import com.latticeengines.domain.exposed.graph.EdgeDeletionRequest;
import com.latticeengines.domain.exposed.graph.VertexCreationRequest;
import com.latticeengines.domain.exposed.graph.VertexDeletionRequest;

public interface GraphDbService {

    boolean addVertex(String customerSpace, String overrideEnv, String overrideVersion, String overrideTenant,
            VertexCreationRequest request) throws Exception;

    boolean addEdge(String customerSpace, String overrideEnv, String overrideVersion, String overrideTenant,
            List<EdgeCreationRequest> request) throws Exception;

    List<Map<String, String>> checkDirectVertexDependencies(String customerSpace, String overrideEnv,
            String overrideVersion, String overrideTenant, String objectId, String type) throws Exception;

    List<Map<String, String>> checkDeepVertexDependencies(String customerSpace, String overrideEnv,
            String overrideVersion, String overrideTenant, String objectId, String type) throws Exception;

    boolean dropVertex(String customerSpace, String overrideEnv, String overrideVersion, String overrideTenant,
            VertexDeletionRequest request) throws Exception;

    boolean dropEdge(String customerSpace, String overrideEnv, String overrideVersion, String overrideTenant,
            List<EdgeDeletionRequest> request) throws Exception;

    boolean updateVertexProperty(String customerSpace, String overrideEnv, String overrideVersion,
            String overrideTenant, String vertexId, Map<String, String> addOrUpdateProperties,
            List<String> removeProperties) throws Exception;
}
