package com.latticeengines.domain.exposed.admin;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonValue;

public enum LatticeProduct {
    LPA("Lead Prioritization");

    private String name;
    private static Map<String, LatticeProduct> lookup = new HashMap<>();

    static {
        for (LatticeProduct product : LatticeProduct.values()) {
            lookup.put(product.getName(), product);
        }
    }

    LatticeProduct(String name) { this.name = name; }

    public static LatticeProduct fromName(String name) { return lookup.get(name); }

    @JsonValue
    public String getName() {
        return name;
    }
}
