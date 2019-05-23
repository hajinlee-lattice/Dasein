package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ProcessContactStepConfiguration extends BaseProcessEntityStepConfiguration {

    @JsonProperty("entity_match_enabled")
    private boolean entityMatchEnabled;

    @Override
    public BusinessEntity getMainEntity() {
        return BusinessEntity.Contact;
    }

    public boolean isEntityMatchEnabled() {
        return entityMatchEnabled;
    }

    public void setEntityMatchEnabled(boolean entityMatchEnabled) {
        this.entityMatchEnabled = entityMatchEnabled;
    }
}
