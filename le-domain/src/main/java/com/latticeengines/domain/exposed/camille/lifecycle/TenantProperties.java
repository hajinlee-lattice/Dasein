package com.latticeengines.domain.exposed.camille.lifecycle;

public class TenantProperties {
    public TenantProperties(String displayName, String description) {
        this.displayName = displayName;
        this.description = description;
    }

    // Serialization constructor
    public TenantProperties() {
    }

    public String displayName;
    public String description;
}
