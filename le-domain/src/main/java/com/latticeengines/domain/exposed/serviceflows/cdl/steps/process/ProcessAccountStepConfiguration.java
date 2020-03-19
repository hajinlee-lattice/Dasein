package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ProcessAccountStepConfiguration extends BaseProcessEntityStepConfiguration {

    @JsonProperty("data_cloud_version")
    private String dataCloudVersion;

    @JsonProperty("allow_internal_enrich_attrs")
    private boolean allowInternalEnrichAttrs;

    @JsonProperty("full_profile")
    private boolean fullProfile;

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

    public boolean isAllowInternalEnrichAttrs() {
        return allowInternalEnrichAttrs;
    }

    public void setAllowInternalEnrichAttrs(boolean allowInternalEnrichAttrs) {
        this.allowInternalEnrichAttrs = allowInternalEnrichAttrs;
    }

    public boolean isFullProfile() {
        return fullProfile;
    }

    public void setFullProfile(boolean fullProfile) {
        this.fullProfile = fullProfile;
    }

}
