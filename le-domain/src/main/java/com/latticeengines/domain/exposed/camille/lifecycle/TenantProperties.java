package com.latticeengines.domain.exposed.camille.lifecycle;

public class TenantProperties extends BaseProperties {
    public Long created;
    public Long lastModified;
    public Long expiredTime;
    public String status;
    public String tenantType;
    public String contract;
    public String userName;
    public TenantProperties(String displayName, String description) {
        super(displayName, description);
        this.displayName = displayName;
        this.description = description;
    }
    // Serialization constructor
    public TenantProperties() {
    }
}
