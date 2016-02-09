package com.latticeengines.serviceflows.workflow.report;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;

public class BaseReportStepConfiguration extends MicroserviceStepConfiguration {
    @NotNull
    @NotEmptyString
    @JsonProperty
    private String internalResourceHostPort;

    @NotNull
    @NotEmptyString
    @JsonProperty
    private String reportName;

    public String getInternalResourceHostPort() {
        return internalResourceHostPort;
    }

    public void setInternalResourceHostPort(String internalResourceHostPort) {
        this.internalResourceHostPort = internalResourceHostPort;
    }

    public String getReportName() {
        return reportName;
    }

    public void setReportName(String reportName) {
        this.reportName = reportName;
    }
}
