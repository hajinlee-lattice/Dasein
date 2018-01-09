package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;

public class CleanupConfig extends TransformerConfig {

    @JsonProperty("base_file")
    private String baseFile;

    @JsonProperty("operation_type")
    private CleanupOperationType operationType;

    public String getBaseFile() {
        return baseFile;
    }

    public void setBaseFile(String baseFile) {
        this.baseFile = baseFile;
    }

    public CleanupOperationType getOperationType() {
        return operationType;
    }

    public void setOperationType(CleanupOperationType operationType) {
        this.operationType = operationType;
    }
}
