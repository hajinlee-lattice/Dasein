package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ProcessTransactionStepConfiguration extends BaseProcessEntityStepConfiguration {

    @JsonProperty("actionIds")
    private List<Long> actionIds;
    @JsonProperty("entity_match_enabled")
    private boolean entityMatchEnabled;
    @JsonProperty("per_tenant_match_report_enabled")
    private boolean perTenantMatchReportEnabled = false;

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

    public boolean isEntityMatchEnabled() {
        return entityMatchEnabled;
    }

    public void setEntityMatchEnabled(boolean entityMatchEnabled) {
        this.entityMatchEnabled = entityMatchEnabled;
    }

    public boolean isPerTenantMatchReportEnabled() {
        return perTenantMatchReportEnabled;
    }

    public void setPerTenantMatchReportEnabled(boolean perTenantMatchReportEnabled) {
        this.perTenantMatchReportEnabled = perTenantMatchReportEnabled;
    }
}
