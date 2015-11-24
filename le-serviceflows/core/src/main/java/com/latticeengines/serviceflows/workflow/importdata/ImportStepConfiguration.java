package com.latticeengines.serviceflows.workflow.importdata;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;

public class ImportStepConfiguration extends MicroserviceStepConfiguration {

    @NotNull
    private SourceType sourceType;

    @JsonProperty("source_type")
    public SourceType getSourceType() {
        return sourceType;
    }

    @JsonProperty("source_type")
    public void setSourceType(SourceType sourceType) {
        this.sourceType = sourceType;
    }

}
