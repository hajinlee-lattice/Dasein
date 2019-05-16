package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.PrepareImportConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance.DeleteFileUploadStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance.OperationExecuteConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.InputFileValidatorConfiguration;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "name")
@JsonSubTypes({
        @Type(value = BaseDataFlowReportStepConfiguration.class, name = "BaseDataFlowReportStepConfiguration"),
        @Type(value = DeleteFileUploadStepConfiguration.class, name = "DeleteFileUploadStepConfiguration"),
        @Type(value = OperationExecuteConfiguration.class, name = "OperationExecuteConfiguration"),
        @Type(value = InputFileValidatorConfiguration.class, name = "InputFileValidatorConfiguration"),
        @Type(value = PrepareImportConfiguration.class, name = "PrepareImportConfiguration"), })
public class BaseReportStepConfiguration extends MicroserviceStepConfiguration {
    @NotNull
    @NotEmptyString
    @JsonProperty("internal_resource_host_port")
    private String internalResourceHostPort;

    @JsonProperty("report_name_prefix")
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
