package com.latticeengines.domain.exposed.pls;

import java.util.HashMap;
import java.util.Map;

import io.swagger.annotations.ApiModel;

@ApiModel("Represents ModelingMethod Enum values")
public enum ModelingMethod {

	PROPENSITY("Propensity"), //
	EXPECTED_VALUE("ExpectedValue");

    private String modelType;
    private static Map<String, ModelingMethod> map = new HashMap<>();

    static {
        for (ModelingMethod m : ModelingMethod.values()) {
            map.put(m.getModelType(), m);
        }
    }

    ModelingMethod(String modelType) {
        this.modelType = modelType;
    }

    public String getModelType() {
        return modelType;
    }

    public static ModelingMethod getByModelType(String modelType) {
        return map.get(modelType);
    }

}
