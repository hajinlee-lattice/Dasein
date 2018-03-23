package com.latticeengines.domain.exposed.serviceapps.core;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AttrConfigRequest {

    @JsonProperty("attr_configs")
    private List<AttrConfig> attrConfigs;

    @JsonProperty("validation_details")
    private ValidationDetails details;

    public List<AttrConfig> getAttrConfigs() {
        return attrConfigs;
    }

    public void setAttrConfigs(List<AttrConfig> attrConfigs) {
        this.attrConfigs = attrConfigs;
    }

    public ValidationDetails getDetails() {
        return details;
    }

    public void setDetails(ValidationDetails details) {
        this.details = details;
    }



}
