package com.latticeengines.domain.exposed.workflow;

public class WorkflowId {

    private long id;

    public WorkflowId(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (id ^ (id >>> 32));
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
        WorkflowId other = (WorkflowId) obj;
        if (id != other.id)
            return false;
        return true;
    }

}
