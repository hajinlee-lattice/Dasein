package com.latticeengines.domain.exposed.camille.lifecycle;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TenantProperties {
    public TenantProperties(String displayName, String description) {
        this.displayName = displayName;
        this.description = description;
    }

    // Serialization constructor
    public TenantProperties() {
    }

    @JsonProperty("DisplayName")
    public String displayName;
    
    @JsonProperty("Description")
    public String description;
}
