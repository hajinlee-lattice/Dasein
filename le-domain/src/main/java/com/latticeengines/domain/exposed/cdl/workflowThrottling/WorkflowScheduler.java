package com.latticeengines.domain.exposed.cdl.workflowThrottling;

import java.util.List;

public interface WorkflowScheduler {

    String GLOBAL = "global";

    ThrottlingResult schedule(WorkflowThrottlingSystemStatus status, List<WorkflowJobSchedulingObject> workflowJobSchedulingObjects, String podid, String division);
}
