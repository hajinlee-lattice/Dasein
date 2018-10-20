package com.latticeengines.domain.exposed.camille.lifecycle;

public class TenantInfo {
    public TenantProperties properties;

    public TenantInfo(TenantProperties properties) {
        this.properties = properties;
    }

    // constructor for serialization
    public TenantInfo() {
    }
}
