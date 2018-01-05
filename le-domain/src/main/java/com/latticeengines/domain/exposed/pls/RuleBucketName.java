package com.latticeengines.domain.exposed.pls;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonValue;

public enum RuleBucketName {
    A_PLUS("A+", 95D), //
    A("A", 70.0D), //
    B("B", 40.0D), //
    C("C", 20.0D), //
    D("D", 10.0D), //
    F("F", 5.0D);

    private String name;
    private Double defaultLikelihood;

    RuleBucketName(String name, Double defaultLikelihood) {
        this.name = name;
        this.defaultLikelihood = defaultLikelihood;
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
