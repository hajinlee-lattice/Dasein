package com.latticeengines.domain.exposed.admin;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;

public class TenantRegistration {

    private CustomerSpaceInfo spaceInfo;
    private List<SerializableDocumentDirectory> configDirectories;

    @JsonProperty("CustomerSpaceInfo")
    public CustomerSpaceInfo getSpaceProperties() { return spaceInfo; }

    @JsonProperty("CustomerSpaceInfo")
    public void setSpaceProperties(CustomerSpaceInfo spaceInfo) {
        this.spaceInfo = spaceInfo;
    }

    @JsonProperty("ConfigDirectories")
    public List<SerializableDocumentDirectory> getConfigDirectories() { return configDirectories; }

    @JsonProperty("ConfigDirectories")
    public void setConfigDirectories(List<SerializableDocumentDirectory> configDirectories) {
        this.configDirectories = configDirectories;
    }
}
