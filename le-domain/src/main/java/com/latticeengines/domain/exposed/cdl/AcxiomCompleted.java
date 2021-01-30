package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AcxiomCompleted extends EventDetail {

    public AcxiomCompleted() {
        super("Acxiom");
    }

    @JsonProperty("processed_count")
    private Long processedCount;

    @JsonProperty("failed_count")
    private Long failedCount;

    public Long getProcessedCount() {
        return processedCount;
    }

    public void setProcessedCount(Long processedCount) {
        this.processedCount = processedCount;
    }

    public Long getFailedCount() {
        return failedCount;
    }

    public void setFailedCount(Long failedCount) {
        this.failedCount = failedCount;
    }

}
