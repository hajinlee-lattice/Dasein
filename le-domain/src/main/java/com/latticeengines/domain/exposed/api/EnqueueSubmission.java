package com.latticeengines.domain.exposed.api;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

public class EnqueueSubmission {

    private Long workflowJobPId;

    public EnqueueSubmission() {
    }

    public EnqueueSubmission(Long workflowJobPId) {
        setWorkflowJobPId(workflowJobPId);
    }

    @JsonProperty("workflowJobPId")
    public Long getWorkflowJobPId() {
        return workflowJobPId;
    }

    @JsonIgnore
    public void setWorkflowJobPId(Long workflowJobPId) {
        this.workflowJobPId = workflowJobPId;
    }
}
