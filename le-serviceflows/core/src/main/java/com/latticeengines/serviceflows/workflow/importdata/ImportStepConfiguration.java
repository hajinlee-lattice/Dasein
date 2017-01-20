package com.latticeengines.serviceflows.workflow.importdata;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;

public class ImportStepConfiguration extends MicroserviceStepConfiguration {

    @NotNull
    @JsonProperty("source_type")
    private SourceType sourceType;
    
    @JsonProperty("source_file_name")
    private String sourceFileName;

    
    public SourceType getSourceType() {
        return sourceType;
    }

    public void setSourceType(SourceType sourceType) {
        this.sourceType = sourceType;
    }

    
    public String getSourceFileName() {
        return sourceFileName;
    }

    public void setSourceFileName(String sourceFileName) {
        this.sourceFileName = sourceFileName;
    }


}
