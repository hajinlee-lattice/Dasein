package com.latticeengines.domain.exposed.graph;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

public class EdgeProperties2 {
    private String tenantId;
    private String behaviorOnDepCheckTraversal;
    private String behaviorOnDeleteOfInVertex;

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

    public Map<String, String> generatePropertMap() {
        Map<String, String> propMap = new HashMap<>();
        if (StringUtils.isNotBlank(tenantId)) {
            propMap.put(GraphConstants.TENANT_ID_PROP_KEY, tenantId);
        }
        if (StringUtils.isNotBlank(behaviorOnDepCheckTraversal)) {
            propMap.put(GraphConstants.BEHAVIOR_ON_DEP_CHECK_TRAVERSAL_KEY, behaviorOnDepCheckTraversal);
        }
        if (StringUtils.isNotBlank(behaviorOnDeleteOfInVertex)) {
            propMap.put(GraphConstants.BEHAVIOR_ON_DELETE_OF_IN_VERTEX_KEY, behaviorOnDeleteOfInVertex);
        }
        return propMap;
    }

    public static EdgeProperties2 generateEdgePropertiesFromPropMap(Map<String, String> propMap) {
        EdgeProperties2 edgeProperties = new EdgeProperties2();
        if (MapUtils.isNotEmpty(propMap)) {
            if (StringUtils.isNotBlank(propMap.get(GraphConstants.TENANT_ID_PROP_KEY))) {
                edgeProperties.setTenantId(propMap.get(GraphConstants.TENANT_ID_PROP_KEY));
            }
            if (StringUtils.isNotBlank(propMap.get(GraphConstants.BEHAVIOR_ON_DEP_CHECK_TRAVERSAL_KEY))) {
                edgeProperties
                        .setBehaviorOnDepCheckTraversal(propMap.get(GraphConstants.BEHAVIOR_ON_DEP_CHECK_TRAVERSAL_KEY));
            }
            if (StringUtils.isNotBlank(propMap.get(GraphConstants.BEHAVIOR_ON_DELETE_OF_IN_VERTEX_KEY))) {
                edgeProperties.setBehaviorOnDeleteOfInVertex(propMap.get(GraphConstants.BEHAVIOR_ON_DELETE_OF_IN_VERTEX_KEY));
            }
        }
        return edgeProperties;
    }
}
