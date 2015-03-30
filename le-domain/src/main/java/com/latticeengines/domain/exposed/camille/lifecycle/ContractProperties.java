package com.latticeengines.domain.exposed.camille.lifecycle;

public class ContractProperties {
    public ContractProperties(String displayName, String description) {
        this.displayName = displayName;
        this.description = description;
    }

    // Serialization constructor
    public ContractProperties() {
    }

    public String displayName;
    public String description;
}
