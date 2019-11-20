package com.latticeengines.domain.exposed.metadata.rention;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

public enum RetentionPolicyTimeUnit {

    DAY(24 * 60 * 60 * 1000l),
    WEEK(7 * 24 * 60 * 60 * 1000l),
    MONTH(30 * 24 * 60 * 60 * 1000l),
    YEAR(365 * 24 * 60 * 60 * 1000l);

    public long getMilliseconds() {
        return milliseconds;
    }

    RetentionPolicyTimeUnit(long count) {
        this.milliseconds = count;
    }

    private final long milliseconds;
    private static Map<String, RetentionPolicyTimeUnit> nameMap;
    private static Set<String> values;

    public static RetentionPolicyTimeUnit fromName(String name) {
        if (StringUtils.isBlank(name)) {
            return null;
        }
        if (values.contains(name)) {
            return valueOf(name);
        } else if (nameMap.containsKey(name)) {
            return nameMap.get(name);
        } else {
            throw new IllegalArgumentException("Cannot find a RetentionPolicyTimeUnit with name " + name);
        }
    }

    static {
        nameMap = new HashMap<>();
        for (RetentionPolicyTimeUnit retentionPolicyTimeUnit : RetentionPolicyTimeUnit.values()) {
            nameMap.put(retentionPolicyTimeUnit.name() + "S", retentionPolicyTimeUnit);
        }
        values = new HashSet<>(
                Arrays.stream(values()).map(retentionPolicyTimeUnit -> retentionPolicyTimeUnit.name()).collect(Collectors.toSet()));
    }

}
