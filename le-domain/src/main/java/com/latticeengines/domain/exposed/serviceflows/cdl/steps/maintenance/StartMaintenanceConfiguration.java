package com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class StartMaintenanceConfiguration extends MicroserviceStepConfiguration {

    @JsonProperty("entity")
    private BusinessEntity entity;

    public BusinessEntity getEntity() {
        return entity;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }
}
