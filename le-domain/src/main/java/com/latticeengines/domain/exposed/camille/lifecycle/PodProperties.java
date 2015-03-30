package com.latticeengines.domain.exposed.camille.lifecycle;

public class PodProperties {
    public PodProperties(String displayName, String description) {
        this.displayName = displayName;
        this.description = description;
    }

    // Serialization constructor
    public PodProperties() {
    }

    public String displayName;
    public String description;
}
