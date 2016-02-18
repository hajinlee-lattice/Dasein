package com.latticeengines.leadprioritization.workflow.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.serviceflows.workflow.dataflow.DataFlowStepConfiguration;

public class DedupEventTableConfiguration extends DataFlowStepConfiguration {
    public DedupEventTableConfiguration() {
        setPurgeSources(true);
    }

    @NotNull
    @NotEmptyString
    @JsonProperty
    private String sourceFileName;

    public String getSourceFileName() {
        return sourceFileName;
    }

    public void setSourceFileName(String sourceFileName) {
        this.sourceFileName = sourceFileName;
    }
}
