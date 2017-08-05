package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;

public class BaseReportStepConfiguration extends MicroserviceStepConfiguration {
    @NotNull
    @NotEmptyString
    @JsonProperty
    private String internalResourceHostPort;

    @JsonProperty
    private String reportNamePrefix;

    public String getInternalResourceHostPort() {
        return internalResourceHostPort;
    }

    public void setInternalResourceHostPort(String internalResourceHostPort) {
        this.internalResourceHostPort = internalResourceHostPort;
    }

    public String getReportNamePrefix() {
        return reportNamePrefix;
    }

    public void setReportNamePrefix(String reportNamePrefix) {
        this.reportNamePrefix = reportNamePrefix;
    }
}
