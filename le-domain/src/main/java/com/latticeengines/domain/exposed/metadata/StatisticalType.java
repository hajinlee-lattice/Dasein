package com.latticeengines.domain.exposed.metadata;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;


public enum StatisticalType {

    INTERVAL("interval"), NOMINAL("nominal"), ORDINAL("ordinal"), RATIO("ratio");

    private final String name;
    private static Map<String, StatisticalType> nameMap;
    private static Set<String> values;

    static {
        nameMap = new HashMap<>();
        for (StatisticalType statisticalType: StatisticalType.values()) {
            nameMap.put(statisticalType.getName(), statisticalType);
        }
        values = new HashSet<>(Arrays.stream(values()).map(StatisticalType::name).collect(Collectors.toSet()));
    }

    StatisticalType(String name) {
        this.name = name;
    }

    public String toString() { return this.name; }

    public String getName() { return this.name; }

    public static StatisticalType fromName(String name) {
        if (StringUtils.isBlank(name)) {
            return null;
        }
        if (values.contains(name.toUpperCase())) {
            return valueOf(name.toUpperCase());
        } else if (nameMap.containsKey(name)) {
            return nameMap.get(name);
        } else {
            throw new IllegalArgumentException("Cannot find a StatisticalType with name " + name);
        }
    }

}
