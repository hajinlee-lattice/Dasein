package com.latticeengines.domain.exposed.pls;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonValue;

public enum RuleBucketName {
    A("A"), //
    A_MINUS("A-"), //
    B("B"), //
    C("C"), //
    D("D"), //
    F("F");

    private String name;

    RuleBucketName(String name) {
        this.name = name;
    }

    private static Map<String, RuleBucketName> ruleBucketNameMap = new HashMap<>();

    static {
        for (RuleBucketName ruleBucketName : values()) {
            ruleBucketNameMap.put(ruleBucketName.getName(), ruleBucketName);
        }
    }

    @JsonValue
    public String getName() {
        return this.name;
    }

    public static Set<String> getDefaultRuleBucketNames() {
        return ruleBucketNameMap.keySet();
    }

    public static RuleBucketName getRuleBucketName(String name) {
        return ruleBucketNameMap.get(name);
    }

}
