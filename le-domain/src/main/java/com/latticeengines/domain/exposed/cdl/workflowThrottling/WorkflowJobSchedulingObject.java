package com.latticeengines.domain.exposed.cdl.workflowThrottling;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingConstraints.WorkflowThrottlingConstraint;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;

public class WorkflowJobSchedulingObject {
    private WorkflowJob workflowJob;
    private List<WorkflowThrottlingConstraint> constraints;

    public WorkflowJobSchedulingObject(WorkflowJob workflowJob, List<WorkflowThrottlingConstraint> constraints) {
        this.workflowJob = workflowJob;
        this.constraints = new ArrayList<WorkflowThrottlingConstraint>() {{
            addAll(constraints);
        }};
    }

    public WorkflowJob getWorkflowJob() {
        return workflowJob;
    }

    public void setWorkflowJob(WorkflowJob workflowJob) {
        this.workflowJob = workflowJob;
    }

    public List<WorkflowThrottlingConstraint> getConstraints() {
        return constraints;
    }

    public void setConstraints(List<WorkflowThrottlingConstraint> constraints) {
        this.constraints = constraints;
    }
}
