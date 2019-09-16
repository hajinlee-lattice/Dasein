package com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingConstraints;

import java.util.Map;

import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingSystemStatus;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingUtils;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;

public class NotExceedingTenantQuota implements WorkflowThrottlingConstraint {
    @Override
    public boolean satisfied(WorkflowThrottlingSystemStatus status, WorkflowJob workflowJob, String podid,
            String division) {
        String customerSpace = workflowJob.getTenant().getId();
        String workflowType = workflowJob.getType();
        Map<JobStatus, Integer> tenantMap = WorkflowThrottlingUtils.getTenantMap(status.getConfig().getTenantLimit(), customerSpace,
                workflowType);
        Integer running = 0;
        if (status.getTenantRunningWorkflow().get(customerSpace) != null) {
            running = status.getTenantRunningWorkflow().get(customerSpace).getOrDefault(workflowType, 0);
        }
        return running < tenantMap.get(JobStatus.RUNNING);
    }
}
