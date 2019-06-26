package com.latticeengines.domain.exposed.datacloud.publication;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DynamoDestination extends PublicationDestination {

    // If not provided, will use latest datacloud version
    @JsonProperty("Version")
    private String version;

    @Override
    @JsonProperty("DestinationType")
    protected String getDestinationType() {
        return this.getClass().getSimpleName();
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}
