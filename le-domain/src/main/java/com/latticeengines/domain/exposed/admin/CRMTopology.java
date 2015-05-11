package com.latticeengines.domain.exposed.admin;

import com.fasterxml.jackson.annotation.JsonValue;

public enum CRMTopology {
    MARKETO("Marketo"),
    ELOQUA("Eloqua"),
    SFDC("SFDC");

    private String name;
    CRMTopology(String name) { this.name = name; }

    public static CRMTopology fromName(String name) { return CRMTopology.valueOf(name.toUpperCase()); }

    @JsonValue
    public String getName() {
        return name;
    }
}
