package com.latticeengines.cdl.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonValue;

public enum ApsRollingPeriod {
    BUSINESS_WEEK("Business Week"), //
    BUSINESS_MONTH("Business Month"), //
    BUSINESS_QUARTER("Business Quarter"), //
    BUSINESS_YEAR("Business Year");

    private String name;
    private static Map<String, ApsRollingPeriod> lookup = new HashMap<>();
    private static List<String> names;

    static {
        for (ApsRollingPeriod period : ApsRollingPeriod.values()) {
            lookup.put(period.getName(), period);
        }

        names = new ArrayList<>();
        for (ApsRollingPeriod period : ApsRollingPeriod.values()) {
            names.add(period.getName());
        }
    }

    ApsRollingPeriod(String name) {
        this.name = name;
    }

    public static ApsRollingPeriod fromName(String name) {
        return lookup.get(name);
    }

    public static List<String> getNames() {
        return names;
    }

    @JsonValue
    public String getName() {
        return name;
    }

}
