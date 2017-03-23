package com.latticeengines.domain.exposed.metadata;

import java.util.HashMap;
import java.util.Map;

public enum StatisticalType {

    INTERVAL("interval"), NOMINAL("nominal"), ORDINAL("ordinal"), RATIO("ratio");

    private final String name;
    private static Map<String, StatisticalType> nameMap;

    static {
        nameMap = new HashMap<>();
        for (StatisticalType statisticalType: StatisticalType.values()) {
            nameMap.put(statisticalType.getName(), statisticalType);
        }
    }

    StatisticalType(String name) {
        this.name = name;
    }

    public String toString() { return this.name; }

    public String getName() { return this.name; }

    public static StatisticalType fromName(String name) {
        if (name == null) {
            return null;
        }
        return nameMap.get(name);
    }

}
