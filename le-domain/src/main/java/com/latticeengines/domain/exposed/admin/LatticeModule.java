package com.latticeengines.domain.exposed.admin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonValue;

public enum LatticeModule {

    TalkingPoint("Talking Point"), //
    HandHoldPA("Hand-hold PA"),
    CDL("CDL"),
    SSVI("SSVI");

    private static Map<String, LatticeModule> lookup = new HashMap<>();
    private static List<String> names;

    static {
        for (LatticeModule product : LatticeModule.values()) {
            lookup.put(product.getName(), product);
        }

        names = new ArrayList<>();
        for (LatticeModule product : LatticeModule.values()) {
            names.add(product.getName());
        }
    }

    private String name;

    LatticeModule(String name) {
        this.name = name;
    }

    public static LatticeModule fromName(String name) {
        return lookup.get(name);
    }

    public static List<String> getNames() {
        return names;
    }

    @JsonValue
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return name;
    }

}
