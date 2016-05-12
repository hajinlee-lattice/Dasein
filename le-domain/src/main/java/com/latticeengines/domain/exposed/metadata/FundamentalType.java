package com.latticeengines.domain.exposed.metadata;

import java.util.HashMap;
import java.util.Map;

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
    YEAR("year");

    private final String name;
    private static Map<String, FundamentalType> nameMap;

    static {
        nameMap = new HashMap<>();
        for (FundamentalType fundamentalType: FundamentalType.values()) {
            nameMap.put(fundamentalType.getName(), fundamentalType);
        }
    }

    FundamentalType(String name) {
        this.name = name;
    }

    public String getName() { return this.name; }

    public String toString() { return this.name; }

    public static FundamentalType fromName(String name) {
        if (nameMap.containsKey(name)) {
            return nameMap.get(name);
        } else  {
            throw new IllegalArgumentException("Cannot find a FundamentalType with name " + name);
        }
    }

}
