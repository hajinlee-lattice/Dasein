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

    public String getName() { return this.name; }

    public static StatisticalType fromName(String name) {
        if (nameMap.containsKey(name)) {
            return nameMap.get(name);
        } else  {
            throw new IllegalArgumentException("Cannot find a StatisticalType with name " + name);
        }
    }

}
