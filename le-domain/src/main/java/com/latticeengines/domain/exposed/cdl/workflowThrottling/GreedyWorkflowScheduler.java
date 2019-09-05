package com.latticeengines.domain.exposed.cdl.workflowThrottling;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingConstraints.WorkflowThrottlingConstraint;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;

// FCFS based scheduler
public class GreedyWorkflowScheduler implements WorkflowScheduler {

    @Override
    public ThrottlingResult schedule(WorkflowThrottlingSystemStatus status, List<WorkflowJobSchedulingObject> workflowJobSchedulingObjects, String podid, String division) {
        Set<Long> canSubmitWorkflowJobIds = new HashSet<>();
        Set<Long> stillEnqueuedWorkflowJobIds = new HashSet<>();
        Set<String> canSubmitTenantIds = new HashSet<>();
        Set<String> stillEnqueuedTenantIds = new HashSet<>();
        for (WorkflowJobSchedulingObject o : workflowJobSchedulingObjects) {
            Long workflowPid = o.getWorkflowJob().getPid();
            String tenantId = o.getWorkflowJob().getTenant().getId();
            if (checkPassConstraint(o, status, podid, division)) {
                // update status running entry
                addRunning(status, o.getWorkflowJob());
                canSubmitWorkflowJobIds.add(workflowPid);
                canSubmitTenantIds.add(tenantId);
            } else {
                stillEnqueuedWorkflowJobIds.add(workflowPid);
                stillEnqueuedTenantIds.add(tenantId);
            }
        }
        return new ThrottlingResult(stillEnqueuedTenantIds, canSubmitTenantIds, canSubmitWorkflowJobIds, stillEnqueuedWorkflowJobIds);
    }

    private boolean checkPassConstraint(WorkflowJobSchedulingObject obj, WorkflowThrottlingSystemStatus status, String podid, String division) {
        for (WorkflowThrottlingConstraint constraint : obj.getConstraints()) {
            if (!constraint.satisfied(status, obj.getWorkflowJob(), podid, division)) {
                return false;
            }
        }
        return true;
    }

    private void addRunning(WorkflowThrottlingSystemStatus status, WorkflowJob workflowJob) {
        String workflowType = workflowJob.getType();
        String customerSpace = workflowJob.getTenant().getId();

        status.getRunningWorkflowInEnv().put(GLOBAL, status.getRunningWorkflowInEnv().get(GLOBAL) + 1);
        status.getRunningWorkflowInEnv().put(workflowType, status.getRunningWorkflowInEnv().getOrDefault(workflowType, 0) + 1);

        status.getRunningWorkflowInStack().put(GLOBAL, status.getRunningWorkflowInStack().get(GLOBAL) + 1);
        status.getRunningWorkflowInStack().put(workflowType, status.getRunningWorkflowInStack().getOrDefault(workflowType, 0) + 1);

        status.getTenantRunningWorkflow().putIfAbsent(customerSpace, new HashMap<>());
        Map<String, Integer> tenantWorkflowMap = status.getTenantRunningWorkflow().get(customerSpace);
        tenantWorkflowMap.put(GLOBAL, tenantWorkflowMap.getOrDefault(GLOBAL, 0) + 1);
        tenantWorkflowMap.put(workflowType, tenantWorkflowMap.getOrDefault(workflowType, 0) + 1);
    }
}
