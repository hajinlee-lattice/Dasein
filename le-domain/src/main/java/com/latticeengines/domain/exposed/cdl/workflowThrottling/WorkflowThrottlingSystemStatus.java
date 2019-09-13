package com.latticeengines.domain.exposed.cdl.workflowThrottling;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.workflow.WorkflowJob;

public class WorkflowThrottlingSystemStatus {
    // global (cross-type count) entries are not taken into consideration for enqueue/dequeue
    // kept just in case of logging or monitoring

    private final String GLOBAL = WorkflowThrottlingUtils.GLOBAL;

    private WorkflowThrottlingConfiguration config;

    // workflowType(or global = total count including all types) -> count
    private Map<String, Integer> runningWorkflowInEnv; //

    private Map<String, Integer> enqueuedWorkflowInEnv; //

    // customerSpace -> type (or global = total count including all types) -> count
    private Map<String, Map<String, Integer>> tenantRunningWorkflow; //
    private Map<String, Map<String, Integer>> tenantEnqueuedWorkflow; //

    private List<WorkflowJob> enqueuedWorkflowJobs;

    public Map<String, Integer> getRunningWorkflowInEnv() {
        return runningWorkflowInEnv;
    }

    public void setRunningWorkflowInEnv(Map<String, Integer> runningWorkflowInEnv) {
        this.runningWorkflowInEnv = runningWorkflowInEnv;
    }
    public Map<String, Integer> getEnqueuedWorkflowInEnv() {
        return enqueuedWorkflowInEnv;
    }

    public void setEnqueuedWorkflowInEnv(Map<String, Integer> enqueuedWorkflowInEnv) {
        this.enqueuedWorkflowInEnv = enqueuedWorkflowInEnv;
    }

    public Map<String, Map<String, Integer>> getTenantRunningWorkflow() {
        return tenantRunningWorkflow;
    }

    public void setTenantRunningWorkflow(Map<String, Map<String, Integer>> tenantRunningWorkflow) {
        this.tenantRunningWorkflow = tenantRunningWorkflow;
    }

    public Map<String, Map<String, Integer>> getTenantEnqueuedWorkflow() {
        return tenantEnqueuedWorkflow;
    }

    public void setTenantEnqueuedWorkflow(Map<String, Map<String, Integer>> tenantEnqueuedWorkflow) {
        this.tenantEnqueuedWorkflow = tenantEnqueuedWorkflow;
    }

    public WorkflowThrottlingConfiguration getConfig() {
        return config;
    }

    public void setConfig(WorkflowThrottlingConfiguration config) {
        this.config = config;
    }

    public List<WorkflowJob> getEnqueuedWorkflowJobs() {
        return enqueuedWorkflowJobs;
    }

    public void setEnqueuedWorkflowJobs(List<WorkflowJob> enqueuedWorkflowJobs) {
        this.enqueuedWorkflowJobs = enqueuedWorkflowJobs;
    }

    public int getTotalRunningWorkflowInEnv() {
        return runningWorkflowInEnv.get(GLOBAL);
    }

    public int getTotalEnqueuedWorkflowInEnv() {
        return enqueuedWorkflowInEnv.get(GLOBAL);
    }
}
