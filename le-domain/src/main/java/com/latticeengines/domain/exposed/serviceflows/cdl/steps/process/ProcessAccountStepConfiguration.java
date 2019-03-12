package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ProcessAccountStepConfiguration extends BaseProcessEntityStepConfiguration {

    @JsonProperty("data_cloud_version")
    private String dataCloudVersion;
    @JsonProperty("entity_match_enabled")
    private boolean entityMatchEnabled;
    @JsonProperty("data_qupta_limit")
    private Long dataQuotaLimit;

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

    public Long getDataQuotaLimit() {
        return dataQuotaLimit;
    }

    @Override
    public Long getDataQuotaLimit(ProductType type) {
        return null;
    }

    public void setDataQuotaLimit(Long dataQuotaLimit) {
        this.dataQuotaLimit = dataQuotaLimit;
    }
}
