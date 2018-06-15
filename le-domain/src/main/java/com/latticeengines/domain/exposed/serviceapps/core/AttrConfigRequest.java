package com.latticeengines.domain.exposed.serviceapps.core;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

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

    public boolean hasError() {
        return details != null && details.hasError();
    }

    public boolean hasWarning() {
        return details != null && details.hasWarning();
    }

    public void fixJsonDeserialization() {
        if (CollectionUtils.isNotEmpty(attrConfigs)) {
            attrConfigs.forEach(AttrConfig::fixJsonDeserialization);
        }
    }

}
