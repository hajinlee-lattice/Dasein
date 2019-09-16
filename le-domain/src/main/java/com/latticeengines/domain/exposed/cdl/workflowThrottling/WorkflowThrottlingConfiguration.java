package com.latticeengines.domain.exposed.cdl.workflowThrottling;

import java.util.Map;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.workflow.JobStatus;

public class WorkflowThrottlingConfiguration {
    /**
     * workflowType (or default = default config for each type) -> count
     * CustomerSpace (or default = config shared by all tenant) -> workflowType (or default) -> RUNNING/ENQUEUED
     */
    private Map<String, Map<JobStatus, Integer>> globalLimit;
    private Map<String, Map<String, Map<JobStatus, Integer>>> tenantLimit;

    public Map<String, Map<JobStatus, Integer>> getGlobalLimit() {
        return globalLimit;
    }

    public void setGlobalLimit(Map<String, Map<JobStatus, Integer>> globalLimit) {
        this.globalLimit = globalLimit;
    }

    public Map<String, Map<String, Map<JobStatus, Integer>>> getTenantLimit() {
        return tenantLimit;
    }

    public void setTenantLimit(Map<String, Map<String, Map<JobStatus, Integer>>> tenantLimit) {
        this.tenantLimit = tenantLimit;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
