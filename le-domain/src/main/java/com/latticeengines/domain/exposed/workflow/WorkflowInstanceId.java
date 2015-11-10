package com.latticeengines.domain.exposed.workflow;

public class WorkflowInstanceId {

    private long instanceId;

    public WorkflowInstanceId(long executionId) {
        this.instanceId = executionId;
    }

    public long getId() {
        return instanceId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (instanceId ^ (instanceId >>> 32));
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
        WorkflowInstanceId other = (WorkflowInstanceId) obj;
        if (instanceId != other.instanceId)
            return false;
        return true;
    }

}
