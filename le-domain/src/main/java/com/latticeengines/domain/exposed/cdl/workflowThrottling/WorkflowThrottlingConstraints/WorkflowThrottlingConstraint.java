package com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingConstraints;


import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingSystemStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;

/**
 * Define constraint on whether a workflow can be submitted
 */
public interface WorkflowThrottlingConstraint {

    String GLOBAL = "global";
    String DEFAULT = "default";

    /**
     * return true if the constraint is satisfied
     */
    boolean satisfied(WorkflowThrottlingSystemStatus status, WorkflowJob workflowJob, String podid, String division);
}
