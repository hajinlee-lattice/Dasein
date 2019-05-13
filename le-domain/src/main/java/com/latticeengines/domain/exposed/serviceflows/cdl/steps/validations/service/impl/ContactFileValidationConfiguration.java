package com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.InputFileValidationConfiguration;

public class ContactFileValidationConfiguration extends InputFileValidationConfiguration {
    @JsonProperty("enable_entity_match")
    private boolean enableEntityMatch;

    public boolean isEnableEntityMatch() {
        return enableEntityMatch;
    }

    public void setEnableEntityMatch(boolean enableEntityMatch) {
        this.enableEntityMatch = enableEntityMatch;
    }

}
