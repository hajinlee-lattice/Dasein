package com.latticeengines.domain.exposed.graph;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

public class VertexProperties2 {
    private String tenantId;
    private String behaviorOnDepCheckTraversal;
    private String behaviorOnDeleteOfInVertex;

    public static VertexProperties2 generateVertexPropertiesFromPropMap(
            Map<String, String> propMap) {
        VertexProperties2 vertexProperties = new VertexProperties2();
        if (MapUtils.isNotEmpty(propMap)) {
            if (StringUtils.isNotBlank(propMap.get(GraphConstants.TENANT_ID_PROP_KEY))) {
                vertexProperties.setTenantId(propMap.get(GraphConstants.TENANT_ID_PROP_KEY));
            }
            if (StringUtils
                    .isNotBlank(propMap.get(GraphConstants.BEHAVIOR_ON_DEP_CHECK_TRAVERSAL_KEY))) {
                vertexProperties.setBehaviorOnDepCheckTraversal(
                        propMap.get(GraphConstants.BEHAVIOR_ON_DEP_CHECK_TRAVERSAL_KEY));
            }
            if (StringUtils
                    .isNotBlank(propMap.get(GraphConstants.BEHAVIOR_ON_DELETE_OF_IN_VERTEX_KEY))) {
                vertexProperties.setBehaviorOnDeleteOfInVertex(
                        propMap.get(GraphConstants.BEHAVIOR_ON_DELETE_OF_IN_VERTEX_KEY));
            }
        }
        return vertexProperties;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getBehaviorOnDepCheckTraversal() {
        return behaviorOnDepCheckTraversal;
    }

    public void setBehaviorOnDepCheckTraversal(String behaviorOnDepCheckTraversal) {
        this.behaviorOnDepCheckTraversal = behaviorOnDepCheckTraversal;
    }

    public String getBehaviorOnDeleteOfInVertex() {
        return behaviorOnDeleteOfInVertex;
    }

    public void setBehaviorOnDeleteOfInVertex(String behaviorOnDeleteOfInVertex) {
        this.behaviorOnDeleteOfInVertex = behaviorOnDeleteOfInVertex;
    }

    public Map<String, String> generatePropertyMap() {
        Map<String, String> propMap = new HashMap<>();
        if (StringUtils.isNotBlank(tenantId)) {
            propMap.put(GraphConstants.TENANT_ID_PROP_KEY, tenantId);
        }
        if (StringUtils.isNotBlank(behaviorOnDepCheckTraversal)) {
            propMap.put(GraphConstants.BEHAVIOR_ON_DEP_CHECK_TRAVERSAL_KEY,
                    behaviorOnDepCheckTraversal);
        }
        if (StringUtils.isNotBlank(behaviorOnDeleteOfInVertex)) {
            propMap.put(GraphConstants.BEHAVIOR_ON_DELETE_OF_IN_VERTEX_KEY,
                    behaviorOnDeleteOfInVertex);
        }
        return propMap;
    }
}
