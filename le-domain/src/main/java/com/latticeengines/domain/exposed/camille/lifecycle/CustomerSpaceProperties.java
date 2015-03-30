package com.latticeengines.domain.exposed.camille.lifecycle;

public class CustomerSpaceProperties {
    public CustomerSpaceProperties(String displayName, String description, String sfdcOrgId) {
        this.displayName = displayName;
        this.description = description;
        this.sfdcOrgId = sfdcOrgId;
    }

    // Serialization constructor
    public CustomerSpaceProperties() {
    }

    public String displayName;
    public String description;
    public String sfdcOrgId;
}
