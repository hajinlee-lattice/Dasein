package com.latticeengines.domain.exposed.cdl;

import java.util.HashMap;
import java.util.Map;

import io.swagger.annotations.ApiModel;

@ApiModel("Represents type of the prediction by an AI model")
public enum PredictionType {

    PROPENSITY("Propensity"), //
    EXPECTED_VALUE("ExpectedValue");

    private static Map<String, PredictionType> map = new HashMap<>();

    static {
        for (PredictionType m : PredictionType.values()) {
            map.put(m.getModelType(), m);
        }
    }

    private String modelType;

    PredictionType(String modelType) {
        this.modelType = modelType;
    }

    public static PredictionType getByModelType(String modelType) {
        return map.get(modelType);
    }

    public String getModelType() {
        return modelType;
    }

}
