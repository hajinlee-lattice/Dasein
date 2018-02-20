package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ProcessAccountStepConfiguration extends BaseProcessEntityStepConfiguration {

    @JsonProperty("data_cloud_version")
    private String dataCloudVersion;

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
}
