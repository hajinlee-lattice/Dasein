package com.latticeengines.domain.exposed.pls;

import java.util.HashMap;
import java.util.Map;

import io.swagger.annotations.ApiModel;

@ApiModel("Represents ModelWorkflowType JSON Object")
public enum ModelWorkflowType {

	ADEFAULT("ListBased"), 
	CROSS_SELL("Cross-Sell"),
	UP_SELL("Up-Sell"),
	PROSPECTING("Prospecting"),
	RENEWAL("Renewal");

    private String modelWorkflowType;
    private static Map<String, ModelWorkflowType> map = new HashMap<>();

    static {
        for (ModelWorkflowType m : ModelWorkflowType.values()) {
            map.put(m.getModelWorkflowType(), m);
        }
    }

    ModelWorkflowType(String modelWorkflowType) {
        this.modelWorkflowType = modelWorkflowType;
    }

    public String getModelWorkflowType() {
        return modelWorkflowType;
    }

    public static ModelWorkflowType getByModelWorkflowType(String modelWorkflowType) {
        return map.get(modelWorkflowType);
    }

}
