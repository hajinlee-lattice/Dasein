package com.latticeengines.graph.entity;

import java.util.List;
import java.util.Map;

public interface BaseGraphEntityManager {

    boolean addVertex(String vertexId, String label, Map<String, String> properties,
            Map<String, Map<String, Map<String, String>>> outgoingEdgesToVertices) throws Exception;

    boolean addEdge(String label, Map<String, String> properties, String inVertexID, String outVertexID)
            throws Exception;

    List<String> checkDirectVertexDependencies(String vertexId) throws Exception;

    boolean dropVertex(String vertexId, boolean failIfDependencyExist) throws Exception;

    boolean dropEdge(String inVertexId, String outVertexId, String edgeLabel) throws Exception;

    boolean updateVertexProperty(String vertexId, Map<String, String> addOrUpdateProperties,
            List<String> removeProperties) throws Exception;
}
