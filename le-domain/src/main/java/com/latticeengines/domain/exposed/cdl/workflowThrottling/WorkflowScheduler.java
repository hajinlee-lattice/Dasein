package com.latticeengines.domain.exposed.cdl.workflowThrottling;

import java.util.List;

public interface WorkflowScheduler {

    String GLOBAL = "global";

    // WorkflowJob must be enqueued to input division (stack)
    ThrottlingResult schedule(WorkflowThrottlingSystemStatus status, List<WorkflowJobSchedulingObject> workflowJobSchedulingObjects, String podid, String division);
}
