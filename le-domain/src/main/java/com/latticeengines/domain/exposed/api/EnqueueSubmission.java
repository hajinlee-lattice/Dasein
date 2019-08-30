package com.latticeengines.domain.exposed.api;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

public class EnqueueSubmission {

    private Long workflowJobId;

    public EnqueueSubmission() {
    }

    public EnqueueSubmission(Long workflowJobId) {
        setWorkflowJobId(workflowJobId);
    }

    @JsonProperty("workflowJobId")
    public Long getWorkflowJobId() {
        return workflowJobId;
    }

    @JsonIgnore
    public void setWorkflowJobId(Long workflowJobId) {
        this.workflowJobId = workflowJobId;
    }
}
