package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ProcessAccountStepConfiguration extends BaseProcessEntityStepConfiguration {

    @JsonProperty("data_cloud_version")
    private String dataCloudVersion;
    @JsonProperty("entity_match_enabled")
    private boolean entityMatchEnabled;
    @JsonProperty("per_tenant_match_report_enabled")
    private boolean perTenantMatchReportEnabled = false;

    @Override
    public BusinessEntity getMainEntity() {
        return BusinessEntity.Account;
    }

    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
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
