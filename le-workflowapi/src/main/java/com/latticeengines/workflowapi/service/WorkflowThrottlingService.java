package com.latticeengines.workflowapi.service;

import java.util.Set;

import com.latticeengines.domain.exposed.cdl.workflowThrottling.ThrottlingResult;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingConfiguration;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.WorkflowThrottlingSystemStatus;

public interface WorkflowThrottlingService {

    WorkflowThrottlingConfiguration getThrottlingConfig(String podid, String division, Set<String> customerSpaces);

    WorkflowThrottlingSystemStatus constructSystemStatus(String podid, String division);

    boolean isWorkflowThrottlingEnabled(String podid, String division);

    boolean isWorkflowThrottlingRolledOut(String podid, String division, String workflowType);

    boolean shouldEnqueueWorkflow(String podid, String division, String workflowType);

    boolean queueLimitReached(String customerSpace, String workflowType, String podid, String division);

    ThrottlingResult getThrottlingResult(String podid, String division);
}
