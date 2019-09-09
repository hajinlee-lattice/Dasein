package com.latticeengines.domain.exposed.api;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

public class WorkflowSubmission {

    private Long workflowJobPId;

    public WorkflowSubmission() {
    }

    public WorkflowSubmission(Long workflowJobPId) {
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
