package com.latticeengines.domain.exposed.camille.lifecycle;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TenantInfo {
    public TenantInfo(TenantProperties properties) {
        this.properties = properties;
    }

    @JsonProperty("Properties")
    public TenantProperties properties;
}
