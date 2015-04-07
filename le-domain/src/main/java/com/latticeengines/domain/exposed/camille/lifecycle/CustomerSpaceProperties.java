package com.latticeengines.domain.exposed.camille.lifecycle;

public class CustomerSpaceProperties extends BaseProperties {
    public CustomerSpaceProperties(String displayName, String description, String sfdcOrgId) {
        super(displayName, description);
        this.sfdcOrgId = sfdcOrgId;
    }

    // Serialization constructor
    public CustomerSpaceProperties() {
    }

    public String sfdcOrgId;
}
