package com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingConstraints;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingSystemStatus;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;

public class NotExceedingTenantQuota implements WorkflowThrottlingConstraint {
    @Override
    public boolean satisfied(WorkflowThrottlingSystemStatus status, WorkflowJob workflowJob, String podid, String division) {
        // construct tenant specific config
        Map<String, Map<String, Map<JobStatus, Integer>>> tenantConfig = status.getConfig().getTenantConfig();
        String workflowType = workflowJob.getType();
        String customerSpace = workflowJob.getTenant().getId();
        Map<String, Map<JobStatus, Integer>> workflowMap = new HashMap<String, Map<JobStatus, Integer>>() {{
            putAll(tenantConfig.get(GLOBAL));
        }};
        if (tenantConfig.get(customerSpace) != null) {
            workflowMap.putAll(tenantConfig.get(customerSpace));
        }

        Map<String, Integer> tenantRunningWorkflowMap = status.getTenantRunningWorkflow().get(customerSpace);
        Integer tenantRunningGlobalCount = tenantRunningWorkflowMap == null ? 0 : tenantRunningWorkflowMap.get(GLOBAL);
        Integer tenantRunningTypedCount = tenantRunningWorkflowMap == null ? 0 : tenantRunningWorkflowMap.getOrDefault(workflowType, 0);

        Integer typedQuota = workflowMap.get(workflowType) == null ? null : workflowMap.get(workflowType).get(JobStatus.RUNNING);

        if (typedQuota != null && typedQuota > tenantRunningTypedCount) {
            return true;
        }

        Integer defaultQuota = workflowMap.get(GLOBAL).get(JobStatus.RUNNING);
        return defaultQuota > tenantRunningGlobalCount;
    }
}
