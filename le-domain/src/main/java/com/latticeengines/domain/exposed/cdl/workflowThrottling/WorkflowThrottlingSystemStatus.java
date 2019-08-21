package com.latticeengines.domain.exposed.cdl.workflowThrottling;

import java.util.Map;

public class WorkflowThrottlingSystemStatus {

    private final String GLOBAL_KEY = "global";

    // workflowType(or global) -> count
    private Map<String, Integer> runningWorkflowInEnv; //
    private Map<String, Integer> runningWorkflowInStack; //
    private Map<String, Integer> canRunWorkflowInEnv; //
    private Map<String, Integer> canRunWorkflowInStack; //
    private Map<String, Integer> canRunWorkflowInStackSpecific; //

    private Map<String, Integer> enqueuedWorkflowInEnv; //
    private Map<String, Integer> enqueuedWorkflowInStack; //
    private Map<String, Integer> canEnqueueWorkflowInEnv; //
    private Map<String, Integer> canEnqueueWorkflowInStack; //
    private Map<String, Integer> canEnqueueWorkflowInStackSpecific; //


    // customerSpace -> type (or global) -> count
    private Map<String, Map<String, Integer>> tenantRunningWorkflow; //
    private Map<String, Map<String, Integer>> tenantCanRunWorkflow; //
    private Map<String, Map<String, Integer>> tenantEnqueuedWorkflow; //
    private Map<String, Map<String, Integer>> tenantCanEnqueueWorkflow; //

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

    public Map<String, Integer> getCanRunWorkflowInEnv() {
        return canRunWorkflowInEnv;
    }

    public void setCanRunWorkflowInEnv(Map<String, Integer> canRunWorkflowInEnv) {
        this.canRunWorkflowInEnv = canRunWorkflowInEnv;
    }

    public Map<String, Integer> getCanRunWorkflowInStack() {
        return canRunWorkflowInStack;
    }

    public void setCanRunWorkflowInStack(Map<String, Integer> canRunWorkflowInStack) {
        this.canRunWorkflowInStack = canRunWorkflowInStack;
    }

    public Map<String, Integer> getCanRunWorkflowInStackSpecific() {
        return canRunWorkflowInStackSpecific;
    }

    public void setCanRunWorkflowInStackSpecific(Map<String, Integer> canRunWorkflowInStackSpecific) {
        this.canRunWorkflowInStackSpecific = canRunWorkflowInStackSpecific;
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

    public Map<String, Integer> getCanEnqueueWorkflowInEnv() {
        return canEnqueueWorkflowInEnv;
    }

    public void setCanEnqueueWorkflowInEnv(Map<String, Integer> canEnqueueWorkflowInEnv) {
        this.canEnqueueWorkflowInEnv = canEnqueueWorkflowInEnv;
    }

    public Map<String, Integer> getCanEnqueueWorkflowInStack() {
        return canEnqueueWorkflowInStack;
    }

    public void setCanEnqueueWorkflowInStack(Map<String, Integer> canEnqueueWorkflowInStack) {
        this.canEnqueueWorkflowInStack = canEnqueueWorkflowInStack;
    }

    public Map<String, Integer> getCanEnqueueWorkflowInStackSpecific() {
        return canEnqueueWorkflowInStackSpecific;
    }

    public void setCanEnqueueWorkflowInStackSpecific(Map<String, Integer> canEnqueueWorkflowInStackSpecific) {
        this.canEnqueueWorkflowInStackSpecific = canEnqueueWorkflowInStackSpecific;
    }

    public Map<String, Map<String, Integer>> getTenantRunningWorkflow() {
        return tenantRunningWorkflow;
    }

    public void setTenantRunningWorkflow(Map<String, Map<String, Integer>> tenantRunningWorkflow) {
        this.tenantRunningWorkflow = tenantRunningWorkflow;
    }

    public Map<String, Map<String, Integer>> getTenantCanRunWorkflow() {
        return tenantCanRunWorkflow;
    }

    public void setTenantCanRunWorkflow(Map<String, Map<String, Integer>> tenantCanRunWorkflow) {
        this.tenantCanRunWorkflow = tenantCanRunWorkflow;
    }

    public Map<String, Map<String, Integer>> getTenantEnqueuedWorkflow() {
        return tenantEnqueuedWorkflow;
    }

    public void setTenantEnqueuedWorkflow(Map<String, Map<String, Integer>> tenantEnqueuedWorkflow) {
        this.tenantEnqueuedWorkflow = tenantEnqueuedWorkflow;
    }

    public Map<String, Map<String, Integer>> getTenantCanEnqueueWorkflow() {
        return tenantCanEnqueueWorkflow;
    }

    public void setTenantCanEnqueueWorkflow(Map<String, Map<String, Integer>> tenantCanEnqueueWorkflow) {
        this.tenantCanEnqueueWorkflow = tenantCanEnqueueWorkflow;
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
