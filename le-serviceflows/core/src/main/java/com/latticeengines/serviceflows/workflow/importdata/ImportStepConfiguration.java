package com.latticeengines.serviceflows.workflow.importdata;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;

public class ImportStepConfiguration extends MicroserviceStepConfiguration {

    @NotNull
    private SourceType sourceType;
    private String sourceFileName;
    private String internalResourceHostPort;

    @JsonProperty("source_type")
    public SourceType getSourceType() {
        return sourceType;
    }

    @JsonProperty("source_type")
    public void setSourceType(SourceType sourceType) {
        this.sourceType = sourceType;
    }

    @JsonProperty("source_file_name")
    public String getSourceFileName() {
        return sourceFileName;
    }

    @JsonProperty("source_file_name")
    public void setSourceFileName(String sourceFileName) {
        this.sourceFileName = sourceFileName;
    }

    @JsonProperty("internal_resource_host_port")
    public String getInternalResourceHostPort() {
        return internalResourceHostPort;
    }

    @JsonProperty("internal_resource_host_port")
    public void setInternalResourceHostPort(String internalResourceHostPort) {
        this.internalResourceHostPort = internalResourceHostPort;
    }

}
