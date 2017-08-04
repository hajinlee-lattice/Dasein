package com.latticeengines.domain.exposed.metadata;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public enum FundamentalType {

    ALPHA("alpha"),
    BOOLEAN("boolean"),
    NUMERIC("numeric"),
    PERCENTAGE("percentage"),
    ENUM("enum"),
    CURRENCY("currency"),
    EMAIL("email"),
    PHONE("phone"),
    URI("uri"),
    YEAR("year"),
    DATE("date");

    public static final String AVRO_PROP_KEY = "FundamentalType";

    private final String name;
    private static Map<String, FundamentalType> nameMap;
    private static Set<String> values;

    static {
        nameMap = new HashMap<>();
        for (FundamentalType fundamentalType: FundamentalType.values()) {
            nameMap.put(fundamentalType.getName(), fundamentalType);
        }
        values = new HashSet<>(Arrays.stream(values()).map(FundamentalType::name).collect(Collectors.toSet()));
    }

    FundamentalType(String name) {
        this.name = name;
    }

    public String getName() { return this.name; }

    public String toString() { return this.name; }

    public static FundamentalType fromName(String name) {
        if (name == null) {
            return null;
        }
        if (values.contains(name)) {
            return valueOf(name);
        } else if (nameMap.containsKey(name)) {
            return nameMap.get(name);
        } else {
            throw new IllegalArgumentException("Cannot find a FundamentalType with name " + name);
        }
    }

}
