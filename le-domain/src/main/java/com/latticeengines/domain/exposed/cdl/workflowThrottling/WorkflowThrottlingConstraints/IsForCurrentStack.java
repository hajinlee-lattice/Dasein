package com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingConstraints;

import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingSystemStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;

public class IsForCurrentStack implements WorkflowThrottlingConstraint {
    @Override
    public boolean satisfied(WorkflowThrottlingSystemStatus status, WorkflowJob workflowJob, String podid,
            String division) {
        return division.equals(workflowJob.getStack());
    }
}
