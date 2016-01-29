package com.latticeengines.domain.exposed.admin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonValue;

public enum LatticeProduct {
    LPA("Lead Prioritization"), //
    LPA3("Lead Prioritization 3.0"), //
    PD("Prospect Discovery");

    private String name;
    private static Map<String, LatticeProduct> lookup = new HashMap<>();
    private static List<String> names;

    static {
        for (LatticeProduct product : LatticeProduct.values()) {
            lookup.put(product.getName(), product);
        }

        names = new ArrayList<>();
        for (LatticeProduct product : LatticeProduct.values()) {
            names.add(product.getName());
        }
    }

    LatticeProduct(String name) {
        this.name = name;
    }

    public static LatticeProduct fromName(String name) {
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
