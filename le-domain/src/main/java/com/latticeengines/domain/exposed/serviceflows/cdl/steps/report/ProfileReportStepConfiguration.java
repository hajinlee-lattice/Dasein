package com.latticeengines.domain.exposed.serviceflows.cdl.steps.report;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class ProfileReportStepConfiguration extends MicroserviceStepConfiguration {

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("allow_internal_enrich_attrs")
    private boolean allowInternalEnrichAttrs;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public boolean isAllowInternalEnrichAttrs() {
        return allowInternalEnrichAttrs;
    }

    public void setAllowInternalEnrichAttrs(boolean allowInternalEnrichAttrs) {
        this.allowInternalEnrichAttrs = allowInternalEnrichAttrs;
    }
}
