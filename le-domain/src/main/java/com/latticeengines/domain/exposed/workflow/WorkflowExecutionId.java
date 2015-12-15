package com.latticeengines.domain.exposed.workflow;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class WorkflowExecutionId {

    @JsonProperty("executionId")
    private long executionId;

    public WorkflowExecutionId(long executionId) {
        this.executionId = executionId;
    }

    @SuppressWarnings("unused")
    private WorkflowExecutionId() {
        // For Json serialization
    }

    @JsonIgnore
    public long getId() {
        return executionId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (executionId ^ (executionId >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        WorkflowExecutionId other = (WorkflowExecutionId) obj;
        if (executionId != other.executionId)
            return false;
        return true;
    }

}
