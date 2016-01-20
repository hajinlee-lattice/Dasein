package com.latticeengines.domain.exposed.propdata.manage;

import java.util.HashMap;
import java.util.Map;

public enum FundamentalType {

    ALPHA("alpha"), BOOLEAN("boolean"), CURRENCY("currency"), NUMERIC("numeric"), YEAR("year");

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

    public static FundamentalType fromName(String name) {
        return nameMap.get(name);
    }

}
