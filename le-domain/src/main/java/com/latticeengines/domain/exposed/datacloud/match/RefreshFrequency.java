package com.latticeengines.domain.exposed.datacloud.match;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

/**
 * Refresh frequency of DataCloud attribute
 */
public enum RefreshFrequency {
    RELEASE("Release"), // refresh whenever a new datacloud version releases
    WEEK("Week"); // refresh weekly

    private static Map<String, RefreshFrequency> nameMap;
    private static Set<String> values;

    static {
        nameMap = new HashMap<>();
        for (RefreshFrequency frequency : RefreshFrequency.values()) {
            nameMap.put(frequency.getName(), frequency);
        }
        values = new HashSet<>(Arrays.stream(values()).map(RefreshFrequency::name).collect(Collectors.toSet()));
    }

    private final String name;

    RefreshFrequency(String name) {
        this.name = name;
    }

    public static RefreshFrequency fromName(String name) {
        if (StringUtils.isBlank(name)) {
            return null;
        }
        if (values.contains(name.toUpperCase())) {
            return valueOf(name.toUpperCase());
        } else if (nameMap.containsKey(name)) {
            return nameMap.get(name);
        } else {
            return RefreshFrequency.RELEASE;
        }
    }

    public String getName() {
        return this.name;
    }

    @Override
    public String toString() {
        return this.name;
    }
}
