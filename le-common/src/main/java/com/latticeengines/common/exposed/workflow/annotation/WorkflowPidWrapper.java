package com.latticeengines.common.exposed.workflow.annotation;

public class WorkflowPidWrapper {
    private long pid;

    public WorkflowPidWrapper(long pid) {
        this.pid = pid;
    }

    public long getPid() {
        return this.pid;
    }

    public void setPid(long pid) {
        this.pid = pid;
    }
}
