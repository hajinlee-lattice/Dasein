package com.latticeengines.domain.exposed.serviceflows.core.steps;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class CleanupAllStepConfiguration extends MicroserviceStepConfiguration {

    @JsonProperty("entity_set")
    private Set<BusinessEntity> entitySet;

    public Set<BusinessEntity> getEntitySet() {
        return entitySet;
    }

    public void setEntitySet(Set<BusinessEntity> entitySet) {
        this.entitySet = entitySet;
    }
}

