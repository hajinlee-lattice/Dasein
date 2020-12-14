package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ProcessTransactionStepConfiguration extends BaseProcessEntityStepConfiguration {

    @JsonProperty("actionIds")
    private List<Long> actionIds;

    @JsonProperty("entity_match_ga_only")
    private boolean entityMatchGAOnly;

    @JsonProperty("apsRollingPeriod")
    private String apsRollingPeriod;

    @Override
    public BusinessEntity getMainEntity() {
        return BusinessEntity.Transaction;
    }

    public List<Long> getActionIds() {
        return actionIds;
    }

    public void setActionIds(List<Long> actionIds) {
        this.actionIds = actionIds;
    }

    public boolean isEntityMatchGAOnly() {
        return entityMatchGAOnly;
    }

    public void setEntityMatchGAOnly(boolean entityMatchGAOnly) {
        this.entityMatchGAOnly = entityMatchGAOnly;
    }

    public String getApsRollingPeriod() {
        return apsRollingPeriod;
    }

    public void setApsRollingPeriod(String apsRollingPeriod) {
        this.apsRollingPeriod = apsRollingPeriod;
    }
}
