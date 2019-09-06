package com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingConstraints;

import java.util.Map;

import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingSystemStatus;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;

public class NotExceedingEnvQuota implements WorkflowThrottlingConstraint {

    @Override
    public boolean satisfied(WorkflowThrottlingSystemStatus status, WorkflowJob workflowJob, String podid, String division) {
        Map<String, Map<JobStatus, Integer>> envWorkflowMap = status.getConfig().getEnvConfig();
        String workflowType = workflowJob.getType();
        Map<JobStatus, Integer> envGlobalQuota = envWorkflowMap.get(GLOBAL);
        Map<JobStatus, Integer> envTypeQuota = envWorkflowMap.get(workflowType);
        if (envGlobalQuota.get(JobStatus.RUNNING) <= status.getTotalRunningWorkflowInEnv()) {
            return false;
        }
        if (envTypeQuota != null && envTypeQuota.get(JobStatus.RUNNING) <= status.getRunningWorkflowInEnv()
                .getOrDefault(workflowType, 0)) {
            return false;
        }
        return true;
    }
}
