package com.latticeengines.domain.exposed.cdl.workflowThrottling;

import java.util.Map;

public class WorkflowThrottlingSystemStatus {

    private final String GLOBAL_KEY = "global";

    private WorkflowThrottlerConfiguration config;

    // workflowType(or global) -> count
    private Map<String, Integer> runningWorkflowInEnv; //
    private Map<String, Integer> runningWorkflowInStack; //

    private Map<String, Integer> enqueuedWorkflowInEnv; //
    private Map<String, Integer> enqueuedWorkflowInStack; //

    // customerSpace -> type (or global) -> count
    private Map<String, Map<String, Integer>> tenantRunningWorkflow; //
    private Map<String, Map<String, Integer>> tenantEnqueuedWorkflow; //

    public Map<String, Integer> getRunningWorkflowInEnv() {
        return runningWorkflowInEnv;
    }

    public void setRunningWorkflowInEnv(Map<String, Integer> runningWorkflowInEnv) {
        this.runningWorkflowInEnv = runningWorkflowInEnv;
    }

    public Map<String, Integer> getRunningWorkflowInStack() {
        return runningWorkflowInStack;
    }

    public void setRunningWorkflowInStack(Map<String, Integer> runningWorkflowInStack) {
        this.runningWorkflowInStack = runningWorkflowInStack;
    }

    public Map<String, Integer> getEnqueuedWorkflowInEnv() {
        return enqueuedWorkflowInEnv;
    }

    public void setEnqueuedWorkflowInEnv(Map<String, Integer> enqueuedWorkflowInEnv) {
        this.enqueuedWorkflowInEnv = enqueuedWorkflowInEnv;
    }

    public Map<String, Integer> getEnqueuedWorkflowInStack() {
        return enqueuedWorkflowInStack;
    }

    public void setEnqueuedWorkflowInStack(Map<String, Integer> enqueuedWorkflowInStack) {
        this.enqueuedWorkflowInStack = enqueuedWorkflowInStack;
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

    public WorkflowThrottlerConfiguration getConfig() {
        return config;
    }

    public void setConfig(WorkflowThrottlerConfiguration config) {
        this.config = config;
    }

    public int getTotalRunningWorkflowInEnv() {
        return runningWorkflowInEnv.get(GLOBAL_KEY);
    }

    public int getTotalEnqueuedWorkflowInEnv() {
        return enqueuedWorkflowInEnv.get(GLOBAL_KEY);
    }

    public int getTotalRunningWorkflowInStack() {
        return runningWorkflowInStack.get(GLOBAL_KEY);
    }

    public int getTotalEnqueuedWorkflowInStack() {
        return enqueuedWorkflowInStack.get(GLOBAL_KEY);
    }
}
