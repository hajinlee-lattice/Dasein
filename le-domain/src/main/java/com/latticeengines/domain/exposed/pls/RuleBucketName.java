package com.latticeengines.domain.exposed.pls;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonValue;

public enum RuleBucketName {
    A_PLUS("A+", 100D), //
    A("A", 95.0D), //
    B("B", 90.0D), //
    C("C", 85.0D), //
    D("D", 75.0D), //
    F("F", 50.0D);

    private String name;
    private Double defaultLikelihood;

    RuleBucketName(String name, Double defaultLikelihood) {
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

    public Double getDefaultLikelihood() {
        return defaultLikelihood;
    }

    public static Set<String> getDefaultRuleBucketNames() {
        return ruleBucketNameMap.keySet();
    }

    public static RuleBucketName getRuleBucketName(String name) {
        return ruleBucketNameMap.get(name);
    }

}
