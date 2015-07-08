package com.latticeengines.domain.exposed.camille.lifecycle;

public class CustomerSpaceProperties extends BaseProperties {

    public CustomerSpaceProperties(String displayName, String description, String sfdcOrgId, String sandboxSfdcOrgId) {
        super(displayName, description);
        this.sfdcOrgId = sfdcOrgId;
        this.sandboxSfdcOrgId = sandboxSfdcOrgId;
    }

    // Serialization constructor
    public CustomerSpaceProperties() {
    }

    public String sfdcOrgId;
    public String sandboxSfdcOrgId;

}
