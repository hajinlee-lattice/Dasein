package com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingConstraints;

import java.util.Map;

import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingSystemStatus;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;

public class NotExceedingEnvQuota implements WorkflowThrottlingConstraint {

    @Override
    public boolean satisfied(WorkflowThrottlingSystemStatus status, WorkflowJob workflowJob, String podid, String division) {
        String workflowType = workflowJob.getType();
        Map<JobStatus, Integer> envQuotaMap = status.getConfig().getGlobalLimit().getOrDefault(workflowType, status.getConfig().getGlobalLimit().get(DEFAULT));
        return envQuotaMap.get(JobStatus.RUNNING) >= status.getRunningWorkflowInEnv().getOrDefault(workflowType, 0);
    }
}
