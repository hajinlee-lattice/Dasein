package com.latticeengines.domain.exposed.serviceflows.cdl.steps.legacydelete;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;

public class LegacyDeleteStepConfiguration extends BaseWrapperStepConfiguration {

    @NotNull
    @JsonProperty("entity")
    private BusinessEntity entity;

    @JsonProperty("entity_matchga_enabled")
    private boolean entityMatchGAEnabled;

    public BusinessEntity getEntity() {
        return entity;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }

    public boolean isEntityMatchGAEnabled() {
        return entityMatchGAEnabled;
    }

    public void setEntityMatchGAEnabled(boolean entityMatchGAEnabled) {
        this.entityMatchGAEnabled = entityMatchGAEnabled;
    }
}
