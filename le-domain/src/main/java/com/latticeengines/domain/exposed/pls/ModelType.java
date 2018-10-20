package com.latticeengines.domain.exposed.pls;

import java.util.HashMap;
import java.util.Map;

public enum ModelType {

    PMML("PmmlModel"), //
    PYTHONMODEL("PythonScriptModel");

    private static Map<String, ModelType> map = new HashMap<>();

    static {
        for (ModelType m : ModelType.values()) {
            map.put(m.getModelType(), m);
        }
    }

    private String modelType;

    ModelType(String modelType) {
        this.modelType = modelType;
    }

    public static ModelType getByModelType(String modelType) {
        return map.get(modelType);
    }

    public static boolean isPythonTypeModel(String modelType) {
        return modelType.equals(PYTHONMODEL.getModelType());
    }

    public String getModelType() {
        return modelType;
    }
}
