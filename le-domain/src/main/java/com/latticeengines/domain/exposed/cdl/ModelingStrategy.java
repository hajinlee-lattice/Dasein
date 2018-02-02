package com.latticeengines.domain.exposed.cdl;

import java.util.HashMap;
import java.util.Map;

import io.swagger.annotations.ApiModel;

@ApiModel("Represents modeling strategy for a given AI Model")
public enum ModelingStrategy {

    CROSS_SELL_REPEAT_PURCHASE("Cross-Sell-Repeat-Purchase"), //
    CROSS_SELL_FIRST_PURCHASE("Cross-Sell-First-Purchase");

    private String modelingStrategy;
    private static Map<String, ModelingStrategy> map = new HashMap<>();

    static {
        for (ModelingStrategy m : ModelingStrategy.values()) {
            map.put(m.getModelingStrategy(), m);
        }
    }

    ModelingStrategy(String modelingStrategy) {
        this.modelingStrategy = modelingStrategy;
    }

    public String getModelingStrategy() {
        return modelingStrategy;
    }

    public static ModelingStrategy getByModelingStrategy(String modelingStrategy) {
        return map.get(modelingStrategy);
    }

}
