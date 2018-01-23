package com.latticeengines.domain.exposed.cdl;

import java.util.HashMap;
import java.util.Map;

import io.swagger.annotations.ApiModel;

@ApiModel("Represents type of the prediction by an AI model")
public enum PredictionType {

	PROPENSITY("Propensity"), //
	EXPECTED_VALUE("ExpectedValue");

    private String modelType;
    private static Map<String, PredictionType> map = new HashMap<>();

    static {
        for (PredictionType m : PredictionType.values()) {
            map.put(m.getModelType(), m);
        }
    }

    PredictionType(String modelType) {
        this.modelType = modelType;
    }

    public String getModelType() {
        return modelType;
    }

    public static PredictionType getByModelType(String modelType) {
        return map.get(modelType);
    }

}
