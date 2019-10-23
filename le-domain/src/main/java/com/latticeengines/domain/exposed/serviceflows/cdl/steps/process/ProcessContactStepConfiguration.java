package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ProcessContactStepConfiguration extends BaseProcessEntityStepConfiguration {

    @JsonProperty("entity_match_ga_only")
    private boolean entityMatchGAOnly;

    @Override
    public BusinessEntity getMainEntity() {
        return BusinessEntity.Contact;
    }

    public boolean isEntityMatchGAOnly() {
        return entityMatchGAOnly;
    }

    public void setEntityMatchGAOnly(boolean entityMatchGAOnly) {
        this.entityMatchGAOnly = entityMatchGAOnly;
    }
}
