package com.latticeengines.domain.exposed.camille.lifecycle;

public class CustomerSpaceInfo {
    public CustomerSpaceInfo(CustomerSpaceProperties properties, String featureFlags) {
        this.properties = properties;
        this.featureFlags = featureFlags;
    }

    public CustomerSpaceProperties properties;
    public String featureFlags;
}
