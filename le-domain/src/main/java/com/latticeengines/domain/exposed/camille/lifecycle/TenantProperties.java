package com.latticeengines.domain.exposed.camille.lifecycle;


public class TenantProperties extends BaseProperties {
    public TenantProperties(String displayName, String description) {
        super(displayName, description);
        this.displayName = displayName;
        this.description = description;
    }

    // Serialization constructor
    public TenantProperties() {
    }

    public Long created;
    public Long lastModified;
    public String purpose;
}
