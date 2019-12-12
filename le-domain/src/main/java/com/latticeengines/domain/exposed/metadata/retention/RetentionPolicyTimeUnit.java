package com.latticeengines.domain.exposed.metadata.retention;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

public enum RetentionPolicyTimeUnit {

    DAY(TimeUnit.DAYS.toMillis(1)),
    WEEK(TimeUnit.DAYS.toMillis(7)),
    MONTH(TimeUnit.DAYS.toMillis(30)),
    YEAR(TimeUnit.DAYS.toMillis(365));

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
            return null;
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
