package com.latticeengines.domain.exposed.admin;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonValue;

public enum CRMTopology {
    MARKETO("Marketo"), ELOQUA("Eloqua"), SFDC("SFDC");

    private static List<String> names;

    static {
        names = new ArrayList<>();
        for (CRMTopology topology : CRMTopology.values()) {
            names.add(topology.getName());
        }
    }

    private String name;

    CRMTopology(String name) {
        this.name = name;
    }

    public static CRMTopology fromName(String name) {
        return CRMTopology.valueOf(name.toUpperCase());
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
