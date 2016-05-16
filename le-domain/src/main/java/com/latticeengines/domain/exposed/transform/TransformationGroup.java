package com.latticeengines.domain.exposed.transform;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.domain.exposed.metadata.FundamentalType;

public enum TransformationGroup {

    STANDARD("standard"), //
    POC("poc"), // 
    ALL("all"); //

    private final String name;
    private static Map<String, FundamentalType> nameMap;

    static {
        nameMap = new HashMap<>();
        for (FundamentalType fundamentalType : FundamentalType.values()) {
            nameMap.put(fundamentalType.getName(), fundamentalType);
        }
    }

    TransformationGroup(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public String toString() {
        return this.name;
    }
}
