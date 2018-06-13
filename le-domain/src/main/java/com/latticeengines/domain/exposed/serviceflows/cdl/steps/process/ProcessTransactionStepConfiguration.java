package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ProcessTransactionStepConfiguration extends BaseProcessEntityStepConfiguration {

    @Override
    public BusinessEntity getMainEntity() {
        return BusinessEntity.Transaction;
    }

    @JsonProperty("actionIds")
    private List<Long> actionIds;

    public List<Long> getActionIds() {
        return actionIds;
    }

    public void setActionIds(List<Long> actionIds) {
        this.actionIds = actionIds;
    }
}
