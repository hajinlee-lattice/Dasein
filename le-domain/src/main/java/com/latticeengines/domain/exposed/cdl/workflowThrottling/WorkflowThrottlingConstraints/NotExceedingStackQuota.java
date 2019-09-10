package com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingConstraints;

import java.util.Map;

import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingSystemStatus;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;

public class NotExceedingStackQuota implements WorkflowThrottlingConstraint{

    @Override
    public boolean satisfied(WorkflowThrottlingSystemStatus status, WorkflowJob workflowJob, String podid, String division) {
        Map<String, Map<JobStatus, Integer>> stackWorkflowMap = status.getConfig().getStackConfig().get(division);
        String workflowType = workflowJob.getType();
        Map<JobStatus, Integer> stackGlobalQuota = stackWorkflowMap.get(GLOBAL);
        Map<JobStatus, Integer> stackTypeQuota = stackWorkflowMap.get(workflowType);
        if (stackGlobalQuota.get(JobStatus.RUNNING) <= status.getTotalRunningWorkflowInStack()) {
            return false;
        }
        if (stackTypeQuota != null && stackTypeQuota.get(JobStatus.RUNNING) <= status.getRunningWorkflowInStack().getOrDefault(workflowType, 0)) {
            return false;
        }
        return true;
    }
}
