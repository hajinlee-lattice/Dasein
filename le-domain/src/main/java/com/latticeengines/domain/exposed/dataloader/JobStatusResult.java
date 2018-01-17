package com.latticeengines.domain.exposed.dataloader;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JobStatusResult {
    private FinalApplicationStatus status;
    private String diagnostics;

    @JsonProperty("status")
    public FinalApplicationStatus getStatus() {
        return status;
    }

    @JsonProperty("status")
    public void setStatus(FinalApplicationStatus status) {
        this.status = status;
    }

    @JsonProperty("diagnostics")
    public String getDiagnostics() {
        return diagnostics;
    }

    @JsonProperty("diagnostics")
    public void setDiagnostics(String diagnostics) {
        this.diagnostics = diagnostics;
    }
}
