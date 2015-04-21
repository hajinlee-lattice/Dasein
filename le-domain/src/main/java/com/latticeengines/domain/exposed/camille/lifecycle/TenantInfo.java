package com.latticeengines.domain.exposed.camille.lifecycle;

public class TenantInfo {
    public TenantInfo(TenantProperties properties) {
        this.properties = properties;
    }

    // constructor for serialization
    public TenantInfo() {}

    public TenantProperties properties;
    public String contractId;
}
