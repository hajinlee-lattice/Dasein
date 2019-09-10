package com.latticeengines.domain.exposed.cdl.workflowThrottling;

import java.util.Map;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.workflow.JobStatus;

public class WorkflowThrottlingConfiguration {
    /**
     * workflowType (or global) -> RUNNING/ENQUEUED
     * Stack (A/B/C) -> workflowType (or global) -> RUNNING/ENQUEUED
     * CustomerSpace (or global) -> workflowType (or global) -> RUNNING/ENQUEUED
     */
    private Map<String, Map<JobStatus, Integer>> envConfig;

    private Map<String, Map<String, Map<JobStatus, Integer>>> stackConfig;

    private Map<String, Map<String, Map<JobStatus, Integer>>> tenantConfig;

    public Map<String, Map<JobStatus, Integer>> getEnvConfig() {
        return envConfig;
    }

    public void setEnvConfig(Map<String, Map<JobStatus, Integer>> envConfig) {
        this.envConfig = envConfig;
    }

    public Map<String, Map<String, Map<JobStatus, Integer>>> getStackConfig() {
        return stackConfig;
    }

    public void setStackConfig(Map<String, Map<String, Map<JobStatus, Integer>>> stackConfig) {
        this.stackConfig = stackConfig;
    }

    public Map<String, Map<String, Map<JobStatus, Integer>>> getTenantConfig() {
        return tenantConfig;
    }

    public void setTenantConfig(Map<String, Map<String, Map<JobStatus, Integer>>> tenantConfig) {
        this.tenantConfig = tenantConfig;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
